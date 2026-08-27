import asyncio
import json
import socket
import time

import websockets

from .config import AUTO_RECONNECT_INTERVAL, BEACON_INTERVAL, PEER_TIMEOUT, UDP_PORT, get_local_ips
from .logger import log
from .node import MeshNode


class NetworkManager:
    def __init__(self, node: MeshNode):
        self.node = node

    def process_http_request(self, connection, request):
        """Processes incoming HTTP requests prior to WebSocket handshake."""
        conn_hdr = request.headers.get("Connection", "").lower()
        upgrade_hdr = request.headers.get("Upgrade", "").lower()

        # Fix/normalize multi-value Connection header (e.g. "keep-alive, Upgrade")
        if "upgrade" in conn_hdr:
            request.headers["Connection"] = "Upgrade"
            return None

        # If HTTP request is not attempting a WebSocket upgrade (e.g. health checks / curl)
        if upgrade_hdr != "websocket":
            try:
                return connection.respond(
                    200,
                    f"MeshOS Node {self.node.id}. REST API available on http://{self.node.advertise_ip}:{self.node.listen_port + 1}\n"
                )
            except Exception:
                pass
        return None

    async def broadcast_graph(self, exclude: str | None = None):
        self.node._touch_self()
        payload = json.dumps({
            "type": "MESH_SYNC",
            "graph": self.node.graph,
            "graph_version": self.node.graph_version,
            "from": self.node.id,
        })
        dead = []
        for pid, ws in list(self.node.connections.items()):
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
        if dest == self.node.id:
            entry = {
                "from": envelope["src"],
                "msg": envelope["msg"],
                "ts": time.time(),
                "path": envelope.get("path", []),
            }
            self.node.inbox.append(entry)
            log(f"✉ Message from {envelope['src']}: {envelope['msg']}")
            return

        next_hop = self.node.bfs_next_hop(dest)
        if next_hop and next_hop in self.node.connections:
            envelope["path"] = envelope.get("path", []) + [self.node.id]
            try:
                await self.node.connections[next_hop].send(json.dumps(envelope))
                log(f"↳ Forwarded message to {dest} via {next_hop}")
            except Exception as e:
                log(f"Forward failed → {next_hop}: {e}")
        else:
            log(f"No route to {dest}")

    async def register_peer(self, peer_id: str, ws, addr: str):
        async with self.node._lock:
            # Deterministic tie-breaking if both nodes dial each other concurrently
            if peer_id in self.node.connections:
                existing_ws = self.node.connections[peer_id]
                if existing_ws is not ws:
                    if self.node.id > peer_id:
                        try:
                            await ws.close()
                        except Exception:
                            pass
                        return
                    else:
                        self.node.connections[peer_id] = ws
                        self.node.peer_addrs[peer_id] = addr
                        try:
                            await existing_ws.close()
                        except Exception:
                            pass
            else:
                self.node.connections[peer_id] = ws
                self.node.peer_addrs[peer_id] = addr

            self.node._touch_self()
            self.node.known_beacons[peer_id] = time.time()
            if addr and addr != "unknown":
                self.node.save_persistent_peer(addr)

        log(f"✓ Connected & Registered peer: {peer_id} ({addr})")
        await self.broadcast_graph()

    async def unregister_peer(self, peer_id: str, ws=None):
        async with self.node._lock:
            if ws is not None and self.node.connections.get(peer_id) is not ws:
                return
            self.node.connections.pop(peer_id, None)
            self.node.peer_addrs.pop(peer_id, None)
            self.node._touch_self()
        log(f"✗ Peer {peer_id} disconnected")
        await self.broadcast_graph()

    async def ws_handler(self, ws):
        """Handles both inbound and outbound WebSocket sessions."""
        peer_id = None
        try:
            # HELLO handshake
            await ws.send(json.dumps({
                "type": "HELLO",
                "id": self.node.id,
                "addr": self.node.self_addr,
                "name": socket.gethostname(),
            }))
            raw = await asyncio.wait_for(ws.recv(), timeout=8)
            hello = json.loads(raw)
            if hello.get("type") != "HELLO":
                log(f"Handshake failed: unexpected message type {hello.get('type')}")
                await ws.close()
                return

            peer_id = hello["id"]
            peer_addr = hello.get("addr", "unknown")

            if peer_id == self.node.id:
                log(f"Handshake ignored: self-connection attempt from same node_id ({peer_id})")
                await ws.close()
                return

            await self.register_peer(peer_id, ws, peer_addr)

            async for raw in ws:
                data = json.loads(raw)
                mtype = data.get("type")

                if mtype == "MESH_SYNC":
                    changed = self.node.merge_graph(data.get("graph", {}))
                    if changed:
                        await self.broadcast_graph(exclude=peer_id)

                elif mtype == "MSG":
                    await self.handle_incoming_message(data)

                elif mtype == "STATUS":
                    pass

        except Exception as e:
            if peer_id:
                log(f"Session with {peer_id} ended: {e}")
            else:
                log(f"Handshake failed: {e}")
        finally:
            if peer_id:
                await self.unregister_peer(peer_id, ws)

    async def dial(self, url: str):
        if not url or url in self.node.dialing or url == self.node.self_addr:
            return

        now = time.time()
        cooldown_until = self.node.cooldowns.get(url, 0)
        if now < cooldown_until:
            return

        self.node.dialing.add(url)
        try:
            log(f"📡 Dialing {url} ...")
            ws = await websockets.connect(url, open_timeout=5, ping_interval=20)
            self.node.fail_counts.pop(url, None)
            self.node.cooldowns.pop(url, None)
            await self.ws_handler(ws)
        except Exception as e:
            cnt = self.node.fail_counts.get(url, 0) + 1
            self.node.fail_counts[url] = cnt
            backoff = min(300, 10 * (2 ** min(cnt - 1, 5)))
            self.node.cooldowns[url] = now + backoff

            log(f"⚠️ Dial failed {url} (attempt {cnt}, retry in {backoff}s): {e}")

            if cnt >= 8 and url in self.node.persistent_peers:
                log(f"Evicting unresponsive peer from persistent store: {url}")
                self.node.persistent_peers.discard(url)
                self.node.save_persistent_peers_to_disk()
        finally:
            self.node.dialing.discard(url)

    async def refresh_connections(self):
        """Triggers a complete mesh reconnect pass across all known & persistent peer endpoints."""
        self.node._touch_self()
        urls_to_check = set(self.node.persistent_peers)

        for nid, info in self.node.graph.items():
            if nid != self.node.id and info.get("addr") and info["addr"] != self.node.self_addr:
                urls_to_check.add(info["addr"])

        connected_addrs = set(self.node.peer_addrs.values())
        now = time.time()
        triggered = []

        for url in urls_to_check:
            if url != self.node.self_addr and url not in connected_addrs and url not in self.node.dialing:
                if now >= self.node.cooldowns.get(url, 0):
                    triggered.append(url)
                    asyncio.create_task(self.dial(url))

        return {
            "status": "refreshing",
            "active_connections": len(self.node.connections),
            "persistent_peers": list(self.node.persistent_peers),
            "triggered_dials": triggered,
        }

    async def auto_reconnect_loop(self):
        while True:
            await asyncio.sleep(AUTO_RECONNECT_INTERVAL)
            try:
                await self.refresh_connections()
            except Exception as e:
                log(f"Auto-reconnect check error: {e}")

    async def udp_beacon_loop(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        payload = json.dumps({
            "type": "BEACON",
            "id": self.node.id,
            "addr": self.node.self_addr,
            "port": self.node.listen_port,
            "name": socket.gethostname(),
            "ts": time.time(),
        }).encode()

        while True:
            try:
                targets = [
                    ("255.255.255.255", UDP_PORT),
                    ("192.168.0.255", UDP_PORT),
                    ("192.168.1.255", UDP_PORT),
                    ("192.168.43.255", UDP_PORT),
                    ("192.168.44.255", UDP_PORT),
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
                log(f"UDP Beacon error: {e}")

            await asyncio.sleep(BEACON_INTERVAL)

    async def udp_listen_loop(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("", UDP_PORT))
        except Exception as e:
            log(f"❌ Failed to bind UDP listener port {UDP_PORT}: {e}")
            return

        sock.setblocking(False)
        loop = asyncio.get_running_loop()
        log(f"Listening for UDP beacons on port {UDP_PORT}...")

        while True:
            try:
                data, addr = await loop.sock_recvfrom(sock, 2048)
                try:
                    msg = json.loads(data.decode())
                except Exception as e:
                    log(f"Received invalid UDP packet from {addr[0]}: {e}")
                    continue

                if msg.get("type") != "BEACON":
                    continue

                peer_id = msg.get("id")
                peer_addr = msg.get("addr")

                if not peer_id or not peer_addr:
                    continue

                if peer_id == self.node.id:
                    continue

                is_new = peer_id not in self.node.known_beacons
                self.node.known_beacons[peer_id] = time.time()
                self.node.save_persistent_peer(peer_addr)

                if is_new:
                    log(f"🔔 Discovered new UDP beacon from node '{peer_id}' ({peer_addr}) from IP {addr[0]}")

                if peer_id not in self.node.connections and peer_addr not in self.node.dialing:
                    log(f"Auto-initiating connection to discovered peer {peer_addr}")
                    asyncio.create_task(self.dial(peer_addr))

            except asyncio.CancelledError:
                break
            except Exception as e:
                log(f"UDP listener loop error: {e}")
                await asyncio.sleep(0.5)

    async def peer_watchdog(self):
        while True:
            now = time.time()
            offline = [pid for pid, ts in self.node.known_beacons.items() if now - ts > PEER_TIMEOUT]
            for pid in offline:
                self.node.known_beacons.pop(pid, None)
                if pid in self.node.connections:
                    log(f"⏰ Peer {pid} beacon timed out (offline)")
                    await self.unregister_peer(pid)
            await asyncio.sleep(5)


def start_network_services(node: MeshNode, net: NetworkManager):
    asyncio.create_task(net.udp_beacon_loop())
    asyncio.create_task(net.udp_listen_loop())
    asyncio.create_task(net.peer_watchdog())
    asyncio.create_task(net.auto_reconnect_loop())
    asyncio.create_task(net.refresh_connections())
