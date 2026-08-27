#!/usr/bin/env python3
"""
MeshOS - Offline Mesh with Auto-Discovery
=========================================
- Automatic peer discovery (UDP broadcast) → works on Wi-Fi + Bluetooth PAN
- WebSocket mesh with full graph gossip
- Multi-hop message routing (BFS)
- REST API for status / send / inbox
- Live status over WebSocket

Install:
    pip install websockets aiohttp --break-system-packages

Run on every machine:
    python3 mesh_server.py --port 8765
"""

import argparse
import asyncio
import json
import os
import socket
import struct
import time
import uuid
from collections import deque

import websockets
from aiohttp import web

ID_FILE = ".mesh_node_id"
PEERS_FILE = "known_peers.json"
UDP_PORT = 3848          # discovery port
BEACON_INTERVAL = 3.0    # seconds
PEER_TIMEOUT = 15.0      # seconds without beacon → consider offline
AUTO_RECONNECT_INTERVAL = 8.0  # auto-reconnect check cycle (seconds)


def get_or_create_node_id():
    if os.path.exists(ID_FILE):
        with open(ID_FILE) as f:
            return f.read().strip()
    nid = uuid.uuid4().hex[:12]
    with open(ID_FILE, "w") as f:
        f.write(nid)
    return nid


def get_local_ips():
    """Return non-loopback IPv4 addresses (Wi-Fi, LAN, Bluetooth PAN, etc.), prioritizing real LAN IPs over APIPA."""
    raw_ips = []
    
    # Probe primary outbound interface IP
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        if ip and not ip.startswith("127."):
            raw_ips.append(ip)
        s.close()
    except Exception:
        pass

    # Resolve local hostname interfaces
    try:
        host_name = socket.gethostname()
        for info in socket.getaddrinfo(host_name, None, socket.AF_INET):
            ip = info[4][0]
            if ip and not ip.startswith("127.") and ip not in raw_ips:
                raw_ips.append(ip)
    except Exception:
        pass

    def ip_priority(ip: str) -> int:
        if ip.startswith("192.168.") or ip.startswith("10."):
            return 1  # Physical Wi-Fi / LAN / Mobile Hotspot (Highest Priority)
        if ip.startswith("172."):
            return 2  # Often WSL / Hyper-V / Docker virtual adapter on Windows
        if ip.startswith("169.254."):
            return 3  # APIPA / Link-Local (Lowest)
        return 2

    sorted_ips = sorted(set(raw_ips), key=ip_priority)
    return sorted_ips or ["127.0.0.1"]


class MeshNode:
    def __init__(self, node_id: str, listen_host: str, listen_port: int, advertise_host: str | None):
        self.id = node_id
        self.listen_host = listen_host
        self.listen_port = listen_port

        # Prefer real LAN/Bluetooth IP for advertising
        if advertise_host:
            self.advertise_ip = advertise_host
        else:
            candidates = get_local_ips()
            self.advertise_ip = candidates[0]

        self.self_addr = f"ws://{self.advertise_ip}:{listen_port}"

        # Direct connections
        self.connections: dict[str, websockets.WebSocketServerProtocol] = {}
        self.peer_addrs: dict[str, str] = {}

        # Full mesh graph
        self.graph: dict[str, dict] = {
            self.id: {
                "addr": self.self_addr,
                "neighbors": [],
                "last_seen": time.time(),
                "name": socket.gethostname(),
            }
        }
        self.graph_version = 0

        # Discovery & Persistent Store
        self.known_beacons: dict[str, float] = {}  # peer_id -> last_beacon_ts
        self.dialing: set[str] = set()
        self.persistent_peers: set[str] = set()
        self.fail_counts: dict[str, int] = {}
        self.cooldowns: dict[str, float] = {}

        self.inbox: list[dict] = []
        self._lock = asyncio.Lock()
        
        self.load_persistent_peers()

    def load_persistent_peers(self):
        """Loads stored peer URLs from known_peers.json."""
        if os.path.exists(PEERS_FILE):
            try:
                with open(PEERS_FILE, "r") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        for url in data:
                            if url and url != self.self_addr:
                                self.persistent_peers.add(url)
                print(f"[MeshOS] Loaded {len(self.persistent_peers)} persistent peers from {PEERS_FILE}")
            except Exception as e:
                print(f"[MeshOS] Failed loading {PEERS_FILE}: {e}")

    def save_persistent_peers_to_disk(self):
        """Writes persistent peer list to JSON file."""
        try:
            with open(PEERS_FILE, "w") as f:
                json.dump(sorted(list(self.persistent_peers)), f, indent=2)
        except Exception as e:
            print(f"[MeshOS] Failed writing {PEERS_FILE}: {e}")

    def save_persistent_peer(self, url: str):
        """Saves a newly discovered/dialed peer URL to known_peers.json."""
        if not url or url == self.self_addr or url in self.persistent_peers:
            return
        self.persistent_peers.add(url)
        self.save_persistent_peers_to_disk()
        print(f"[MeshOS] Saved new persistent peer: {url}")

    async def refresh_connections(self):
        """Triggers a complete mesh reconnect pass across all known & persistent peer endpoints."""
        self._touch_self()
        urls_to_check = set(self.persistent_peers)

        # Include all peer addresses learned through the full graph gossip
        for nid, info in self.graph.items():
            if nid != self.id and info.get("addr") and info["addr"] != self.self_addr:
                urls_to_check.add(info["addr"])

        connected_addrs = set(self.peer_addrs.values())
        now = time.time()
        triggered = []

        for url in urls_to_check:
            if url != self.self_addr and url not in connected_addrs and url not in self.dialing:
                # Respect backoff cooldown before retrying failed endpoints
                if now >= self.cooldowns.get(url, 0):
                    triggered.append(url)
                    asyncio.create_task(self.dial(url))

        return {
            "status": "refreshing",
            "active_connections": len(self.connections),
            "persistent_peers": list(self.persistent_peers),
            "triggered_dials": triggered,
        }

    async def auto_reconnect_loop(self):
        """Background task that periodically checks and re-establishes broken or missing peer links."""
        while True:
            await asyncio.sleep(AUTO_RECONNECT_INTERVAL)
            try:
                # Re-dial any missing persistent or graph peers
                await self.refresh_connections()
            except Exception as e:
                print(f"[MeshOS] Auto-reconnect check error: {e}")

    # ───────────────────────── Graph helpers ─────────────────────────

    def _touch_self(self):
        self.graph[self.id]["neighbors"] = list(self.connections.keys())
        self.graph[self.id]["last_seen"] = time.time()
        self.graph[self.id]["addr"] = self.self_addr

    def merge_graph(self, other_graph: dict) -> bool:
        changed = False
        for nid, info in other_graph.items():
            if nid == self.id:
                continue
            mine = self.graph.get(nid)
            if mine is None or info.get("last_seen", 0) > mine.get("last_seen", 0):
                self.graph[nid] = info
                changed = True
        if changed:
            self.graph_version += 1
        return changed

    def bfs_next_hop(self, dest_id: str):
        if dest_id == self.id:
            return "SELF"
        if dest_id not in self.graph:
            return None

        visited = {self.id}
        q = deque([(self.id, None)])  # (node, first_hop)

        while q:
            node, first_hop = q.popleft()
            for nb in self.graph.get(node, {}).get("neighbors", []):
                if nb in visited:
                    continue
                visited.add(nb)
                hop = first_hop if first_hop is not None else nb
                if nb == dest_id:
                    return hop
                q.append((nb, hop))
        return None

    def snapshot(self):
        self._touch_self()
        return {
            "self_id": self.id,
            "self_addr": self.self_addr,
            "advertise_ip": self.advertise_ip,
            "graph_version": self.graph_version,
            "direct_peers": list(self.connections.keys()),
            "online_peers": list(self.known_beacons.keys()),
            "graph": self.graph,
        }

    # ───────────────────────── Gossip ─────────────────────────

    async def broadcast_graph(self, exclude: str | None = None):
        self._touch_self()
        payload = json.dumps({
            "type": "MESH_SYNC",
            "graph": self.graph,
            "graph_version": self.graph_version,
            "from": self.id,
        })
        dead = []
        for pid, ws in list(self.connections.items()):
            if pid == exclude:
                continue
            try:
                await ws.send(payload)
            except Exception:
                dead.append(pid)
        for pid in dead:
            await self.unregister_peer(pid)

    async def handle_incoming_message(self, envelope: dict):
        dest = envelope["dest"]
        if dest == self.id:
            entry = {
                "from": envelope["src"],
                "msg": envelope["msg"],
                "ts": time.time(),
                "path": envelope.get("path", []),
            }
            self.inbox.append(entry)
            print(f"[MeshOS] Message from {envelope['src']}: {envelope['msg']}")
            return

        next_hop = self.bfs_next_hop(dest)
        if next_hop and next_hop in self.connections:
            envelope["path"] = envelope.get("path", []) + [self.id]
            try:
                await self.connections[next_hop].send(json.dumps(envelope))
            except Exception as e:
                print(f"[MeshOS] Forward failed → {next_hop}: {e}")
        else:
            print(f"[MeshOS] No route to {dest}")

    # ───────────────────────── Connection lifecycle ─────────────────────────

    async def register_peer(self, peer_id: str, ws, addr: str):
        async with self._lock:
            # Deterministic tie-breaking if both nodes dial each other concurrently
            if peer_id in self.connections:
                existing_ws = self.connections[peer_id]
                if existing_ws is not ws:
                    # Keep the connection initiated by the higher node_id to break tie cleanly
                    if self.id > peer_id:
                        # Reject/close the lower-priority duplicate socket
                        try:
                            await ws.close()
                        except Exception:
                            pass
                        return
                    else:
                        # Replace old connection with new one
                        self.connections[peer_id] = ws
                        self.peer_addrs[peer_id] = addr
                        try:
                            await existing_ws.close()
                        except Exception:
                            pass
            else:
                self.connections[peer_id] = ws
                self.peer_addrs[peer_id] = addr

            self._touch_self()
            self.known_beacons[peer_id] = time.time()
            if addr and addr != "unknown":
                self.save_persistent_peer(addr)

        print(f"[MeshOS] ✓ Connected to {peer_id} ({addr})")
        await self.broadcast_graph()

    async def unregister_peer(self, peer_id: str, ws=None):
        async with self._lock:
            # ONLY remove from self.connections if the socket being closed is the ACTIVE socket!
            if ws is not None and self.connections.get(peer_id) is not ws:
                return
            self.connections.pop(peer_id, None)
            self.peer_addrs.pop(peer_id, None)
            self._touch_self()
        print(f"[MeshOS] ✗ Peer {peer_id} disconnected")
        await self.broadcast_graph()

    async def ws_handler(self, ws):
        """Handles both inbound and outbound WebSocket sessions."""
        peer_id = None
        try:
            # HELLO handshake
            await ws.send(json.dumps({
                "type": "HELLO",
                "id": self.id,
                "addr": self.self_addr,
                "name": socket.gethostname(),
            }))
            raw = await asyncio.wait_for(ws.recv(), timeout=8)
            hello = json.loads(raw)
            if hello.get("type") != "HELLO":
                await ws.close()
                return

            peer_id = hello["id"]
            peer_addr = hello.get("addr", "unknown")

            if peer_id == self.id:
                await ws.close()
                return

            await self.register_peer(peer_id, ws, peer_addr)

            async for raw in ws:
                data = json.loads(raw)
                mtype = data.get("type")

                if mtype == "MESH_SYNC":
                    changed = self.merge_graph(data.get("graph", {}))
                    if changed:
                        await self.broadcast_graph(exclude=peer_id)

                elif mtype == "MSG":
                    await self.handle_incoming_message(data)

                elif mtype == "STATUS":
                    pass

        except Exception as e:
            if peer_id:
                print(f"[MeshOS] Session with {peer_id} ended: {e}")
        finally:
            if peer_id:
                await self.unregister_peer(peer_id, ws)

    async def dial(self, url: str):
        if not url or url in self.dialing or url == self.self_addr:
            return

        now = time.time()
        cooldown_until = self.cooldowns.get(url, 0)
        if now < cooldown_until:
            return

        self.dialing.add(url)
        try:
            print(f"[MeshOS] Dialing {url} ...")
            ws = await websockets.connect(url, open_timeout=4, ping_interval=20)
            self.fail_counts.pop(url, None)
            self.cooldowns.pop(url, None)
            await self.ws_handler(ws)
        except Exception as e:
            cnt = self.fail_counts.get(url, 0) + 1
            self.fail_counts[url] = cnt
            # Exponential backoff: 10s, 20s, 40s, 80s, max 300s
            backoff = min(300, 10 * (2 ** min(cnt - 1, 5)))
            self.cooldowns[url] = now + backoff

            print(f"[MeshOS] Dial failed {url} (attempt {cnt}, retry in {backoff}s): {e}")

            # Evict dead persistent peer after 8 consecutive failures
            if cnt >= 8 and url in self.persistent_peers:
                print(f"[MeshOS] Evicting unresponsive peer from persistent store: {url}")
                self.persistent_peers.discard(url)
                self.save_persistent_peers_to_disk()
        finally:
            self.dialing.discard(url)

    # ───────────────────────── UDP Auto-Discovery ─────────────────────────

    async def udp_beacon_loop(self):
        """Broadcast our presence every few seconds."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        payload = json.dumps({
            "type": "BEACON",
            "id": self.id,
            "addr": self.self_addr,
            "port": self.listen_port,
            "name": socket.gethostname(),
            "ts": time.time(),
        }).encode()

        while True:
            try:
                # Broadcast to common subnets + limited broadcast
                targets = [
                    ("255.255.255.255", UDP_PORT),
                    ("192.168.0.255", UDP_PORT),
                    ("192.168.1.255", UDP_PORT),
                    ("192.168.43.255", UDP_PORT),   # common Android hotspot
                    ("192.168.44.255", UDP_PORT),   # common Bluetooth PAN
                    ("10.0.0.255", UDP_PORT),
                ]
                for ip in get_local_ips():
                    parts = ip.split(".")
                    if len(parts) == 4:
                        targets.append((f"{parts[0]}.{parts[1]}.{parts[2]}.255", UDP_PORT))

                for target in set(targets):
                    try:
                        sock.sendto(payload, target)
                    except Exception:
                        pass
            except Exception as e:
                print(f"[MeshOS] Beacon error: {e}")

            await asyncio.sleep(BEACON_INTERVAL)

    async def udp_listen_loop(self):
        """Listen for beacons and auto-dial new peers."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", UDP_PORT))
        sock.setblocking(False)

        loop = asyncio.get_running_loop()

        while True:
            try:
                data, addr = await loop.sock_recvfrom(sock, 2048)
                msg = json.loads(data.decode())
                if msg.get("type") != "BEACON":
                    continue

                peer_id = msg["id"]
                peer_addr = msg["addr"]

                if peer_id == self.id:
                    continue

                self.known_beacons[peer_id] = time.time()
                self.save_persistent_peer(peer_addr)

                # Auto-connect if we don't already have a live connection
                if peer_id not in self.connections and peer_addr not in self.dialing:
                    asyncio.create_task(self.dial(peer_addr))

            except Exception:
                await asyncio.sleep(0.05)

    async def peer_watchdog(self):
        """Mark peers offline if beacons stop arriving."""
        while True:
            now = time.time()
            offline = [pid for pid, ts in self.known_beacons.items() if now - ts > PEER_TIMEOUT]
            for pid in offline:
                self.known_beacons.pop(pid, None)
                if pid in self.connections:
                    await self.unregister_peer(pid)
            await asyncio.sleep(5)


# ───────────────────────── REST API ─────────────────────────

def make_app(node: MeshNode):
    app = web.Application()

    async def connection_info(request):
        return web.json_response(node.snapshot())

    async def peers(request):
        return web.json_response({
            "direct": node.peer_addrs,
            "known_beacons": list(node.known_beacons.keys()),
            "persistent_peers": list(node.persistent_peers),
        })

    async def inbox(request):
        return web.json_response(node.inbox[-50:])  # last 50

    async def send_to(request):
        if request.method == "POST":
            body = await request.json()
            dest = body.get("to")
            msg = body.get("msg")
        else:
            dest = request.query.get("to")
            msg = request.query.get("msg")

        if not dest or msg is None:
            return web.json_response({"error": "need 'to' and 'msg'"}, status=400)

        if dest == node.id:
            return web.json_response({"error": "cannot send to self"}, status=400)

        next_hop = node.bfs_next_hop(dest)
        if not next_hop or next_hop not in node.connections:
            return web.json_response({
                "error": f"no route to {dest}",
                "known_nodes": list(node.graph.keys()),
            }, status=404)

        envelope = {
            "type": "MSG",
            "dest": dest,
            "src": node.id,
            "msg": msg,
            "path": [node.id],
        }
        try:
            await node.connections[next_hop].send(json.dumps(envelope))
            return web.json_response({"status": "sent", "via": next_hop})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def connect_route(request):
        body = await request.json()
        url = body.get("url")
        if not url:
            return web.json_response({"error": "need url"}, status=400)
        node.save_persistent_peer(url)
        asyncio.create_task(node.dial(url))
        return web.json_response({"status": "dialing_and_saved", "url": url})

    async def refresh_route(request):
        result = await node.refresh_connections()
        return web.json_response(result)

    app.router.add_get("/connection_info", connection_info)
    app.router.add_get("/peers", peers)
    app.router.add_get("/inbox", inbox)
    app.router.add_route("*", "/send_to", send_to)
    app.router.add_post("/connect", connect_route)
    app.router.add_route("*", "/refresh", refresh_route)
    return app


# ───────────────────────── Main ─────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="MeshOS - Auto-discovering mesh node")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--advertise-host", default=None,
                        help="IP that other nodes should use to reach you")
    parser.add_argument("--connect", action="append", default=[],
                        help="Optional manual seed: ws://ip:port")
    args = parser.parse_args()

    node_id = get_or_create_node_id()
    node = MeshNode(node_id, args.host, args.port, args.advertise_host)

    all_ips = get_local_ips()
    print("=" * 60)
    print(f"  MeshOS Node ID     : {node.id}")
    print(f"  WebSocket          : {node.self_addr}")
    print(f"  REST API           : http://{node.advertise_ip}:{args.port + 1}")
    print(f"  Discovery (UDP)    : port {UDP_PORT}")
    print(f"  Detected IPs       : {', '.join(all_ips)}")
    print("=" * 60)

    # WebSocket server
    ws_server = await websockets.serve(
        node.ws_handler,
        args.host,
        args.port,
        ping_interval=20,
        ping_timeout=20,
    )

    # REST API on port+1
    app = make_app(node)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, args.host, args.port + 1)
    await site.start()

    # Start discovery + maintenance tasks
    asyncio.create_task(node.udp_beacon_loop())
    asyncio.create_task(node.udp_listen_loop())
    asyncio.create_task(node.peer_watchdog())
    asyncio.create_task(node.auto_reconnect_loop())

    # Optional manual seeds
    for url in args.connect:
        node.save_persistent_peer(url)
        asyncio.create_task(node.dial(url))

    # Trigger immediate reconnect pass for all saved persistent peers
    asyncio.create_task(node.refresh_connections())

    print("[MeshOS] Running... (auto-discovery & persistent auto-reconnect active)")
    await asyncio.Future()  # run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[MeshOS] Shutting down")