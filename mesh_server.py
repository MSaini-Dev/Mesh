#!/usr/bin/env python3
"""
MeshOS - Offline Mesh Server
============================
Run this on every laptop/PC. No internet needed - connect over
Wi-Fi hotspot, same LAN, or any network where the machines can
reach each other's IP (Bluetooth PAN also works, it just gives you
an IP like any other NIC).

Each node:
  - has a unique ID (persisted to disk so it survives restarts)
  - keeps a WebSocket server open for incoming peer connections
  - can dial out to other nodes' WebSocket servers
  - gossips the FULL mesh graph (who is connected to who) to every
    peer, so every node ends up with complete mesh knowledge
  - routes messages hop-by-hop using that graph (BFS shortest path),
    so A -> B -> C works even if A and C aren't directly connected
  - exposes a small REST API (aiohttp) on the same port:

        GET  /connection_info      -> this node's id + full mesh graph
        GET  /send_to?to=<id>&msg=<text>   (or POST JSON {"to":..,"msg":..})
        GET  /inbox                -> messages received by this node
        GET  /peers                -> just the direct peer list

Install deps (once, needs internet or a local mirror):
    pip install websockets aiohttp --break-system-packages

Run:
    python3 mesh_server.py --port 8765
    python3 mesh_server.py --port 8765 --connect ws://192.168.1.23:8765

Then on any node, open http://localhost:8765/connection_info in a
browser, or curl it, to see the whole mesh.

To send a message from this node to another node anywhere in the
mesh (direct or multi-hop, doesn't matter, routing is automatic):

    curl "http://localhost:8765/send_to?to=<target_node_id>&msg=hello"
"""

import argparse
import asyncio
import json
import os
import time
import uuid
from collections import deque

import websockets
from aiohttp import web

ID_FILE = ".mesh_node_id"


def get_or_create_node_id():
    if os.path.exists(ID_FILE):
        return open(ID_FILE).read().strip()
    nid = uuid.uuid4().hex[:12]
    with open(ID_FILE, "w") as f:
        f.write(nid)
    return nid


class MeshNode:
    def __init__(self, node_id, listen_host, listen_port):
        self.id = node_id
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.self_addr = f"ws://{listen_host}:{listen_port}"

        # live websocket connections to directly-connected peers
        # peer_id -> websocket
        self.connections = {}
        # peer_id -> ws url (so we can reconnect / show info)
        self.peer_addrs = {}

        # full mesh graph as everyone currently understands it
        # node_id -> {"addr": str, "neighbors": [node_id, ...], "last_seen": ts}
        self.graph = {
            self.id: {"addr": self.self_addr, "neighbors": [], "last_seen": time.time()}
        }
        self.graph_version = 0

        self.inbox = []  # list of {"from":.., "msg":.., "ts":..}
        self._lock = asyncio.Lock()

    # ---------- graph helpers ----------

    def _touch_self(self):
        self.graph[self.id]["neighbors"] = list(self.connections.keys())
        self.graph[self.id]["last_seen"] = time.time()

    def merge_graph(self, other_graph, other_version):
        """Merge a peer's view of the graph into ours. Returns True if changed."""
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

    def bfs_next_hop(self, dest_id):
        """Return the peer_id of the next hop toward dest_id, or None."""
        if dest_id == self.id:
            return "SELF"
        if dest_id not in self.graph:
            return None
        start = self.id
        visited = {start}
        q = deque([(start, None)])  # (node, first_hop_from_self)
        while q:
            node, first_hop = q.popleft()
            neighbors = self.graph.get(node, {}).get("neighbors", [])
            for nb in neighbors:
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
            "graph_version": self.graph_version,
            "direct_peers": list(self.connections.keys()),
            "graph": self.graph,
        }

    # ---------- gossip ----------

    async def broadcast_graph(self, exclude=None):
        self._touch_self()
        payload = json.dumps({
            "type": "MESH_SYNC",
            "graph": self.graph,
            "graph_version": self.graph_version,
        })
        for pid, ws in list(self.connections.items()):
            if pid == exclude:
                continue
            try:
                await ws.send(payload)
            except Exception:
                pass

    async def handle_incoming_message(self, envelope):
        """envelope: {type: MSG, dest, src, msg, path}"""
        dest = envelope["dest"]
        if dest == self.id:
            self.inbox.append({"from": envelope["src"], "msg": envelope["msg"], "ts": time.time()})
            print(f"[MeshOS] Message delivered from {envelope['src']}: {envelope['msg']}")
            return
        # forward it
        next_hop = self.bfs_next_hop(dest)
        if next_hop and next_hop in self.connections:
            envelope["path"].append(self.id)
            try:
                await self.connections[next_hop].send(json.dumps(envelope))
            except Exception:
                print(f"[MeshOS] Failed forwarding to {next_hop}")
        else:
            print(f"[MeshOS] No route to {dest}, dropping message")

    # ---------- connection lifecycle ----------

    async def register_peer(self, peer_id, ws, addr):
        async with self._lock:
            self.connections[peer_id] = ws
            self.peer_addrs[peer_id] = addr
            self._touch_self()
        print(f"[MeshOS] Connected to peer {peer_id} ({addr}). Direct peers: {list(self.connections.keys())}")
        await self.broadcast_graph()

    async def unregister_peer(self, peer_id):
        async with self._lock:
            self.connections.pop(peer_id, None)
            self._touch_self()
        print(f"[MeshOS] Peer {peer_id} disconnected")
        await self.broadcast_graph()

    async def ws_session(self, ws, peer_id_hint=None, dial_addr=None):
        """Shared logic for both inbound-accepted and outbound-dialed connections."""
        try:
            # handshake: exchange HELLO first
            await ws.send(json.dumps({"type": "HELLO", "id": self.id, "addr": self.self_addr}))
            hello_raw = await ws.recv()
            hello = json.loads(hello_raw)
            if hello.get("type") != "HELLO":
                await ws.close()
                return
            peer_id = hello["id"]
            peer_addr = hello.get("addr", dial_addr)

            await self.register_peer(peer_id, ws, peer_addr)

            async for raw in ws:
                data = json.loads(raw)
                mtype = data.get("type")
                if mtype == "MESH_SYNC":
                    changed = self.merge_graph(data["graph"], data["graph_version"])
                    if changed:
                        await self.broadcast_graph(exclude=peer_id)
                elif mtype == "MSG":
                    await self.handle_incoming_message(data)
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            if peer_id_hint:
                await self.unregister_peer(peer_id_hint)
            elif 'peer_id' in dir():
                pass

    async def handle_inbound(self, ws):
        await self.ws_session(ws)

    async def dial(self, url):
        try:
            ws = await websockets.connect(url)
        except Exception as e:
            print(f"[MeshOS] Could not connect to {url}: {e}")
            return
        # we need peer_id to clean up on disconnect -> wrap
        await self._dial_session(ws, url)

    async def _dial_session(self, ws, url):
        peer_id = None
        try:
            await ws.send(json.dumps({"type": "HELLO", "id": self.id, "addr": self.self_addr}))
            hello = json.loads(await ws.recv())
            peer_id = hello["id"]
            peer_addr = hello.get("addr", url)
            await self.register_peer(peer_id, ws, peer_addr)
            async for raw in ws:
                data = json.loads(raw)
                mtype = data.get("type")
                if mtype == "MESH_SYNC":
                    changed = self.merge_graph(data["graph"], data["graph_version"])
                    if changed:
                        await self.broadcast_graph(exclude=peer_id)
                elif mtype == "MSG":
                    await self.handle_incoming_message(data)
        except Exception as e:
            print(f"[MeshOS] Connection to {url} ended: {e}")
        finally:
            if peer_id:
                await self.unregister_peer(peer_id)


# ---------------- REST API ----------------

def make_app(node: MeshNode):
    app = web.Application()

    async def connection_info(request):
        return web.json_response(node.snapshot())

    async def peers(request):
        return web.json_response({"direct_peers": node.peer_addrs})

    async def inbox(request):
        return web.json_response(node.inbox)

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
            return web.json_response({"status": "that's you"}, status=400)

        next_hop = node.bfs_next_hop(dest)
        if not next_hop or next_hop not in node.connections:
            return web.json_response({"error": f"no route to {dest}", "known_nodes": list(node.graph.keys())}, status=404)

        envelope = {"type": "MSG", "dest": dest, "src": node.id, "msg": msg, "path": [node.id]}
        try:
            await node.connections[next_hop].send(json.dumps(envelope))
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

        return web.json_response({"status": "sent", "via_next_hop": next_hop})

    async def connect_route(request):
        # POST {"url": "ws://host:port"} to connect to a new peer at runtime
        body = await request.json()
        url = body.get("url")
        if not url:
            return web.json_response({"error": "need 'url'"}, status=400)
        asyncio.create_task(node.dial(url))
        return web.json_response({"status": "dialing", "url": url})

    app.router.add_get("/connection_info", connection_info)
    app.router.add_get("/peers", peers)
    app.router.add_get("/inbox", inbox)
    app.router.add_get("/send_to", send_to)
    app.router.add_post("/send_to", send_to)
    app.router.add_post("/connect", connect_route)
    return app


# ---------------- main ----------------

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765, help="port for both websocket + REST")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--connect", action="append", default=[], help="ws://ip:port of an existing node, can repeat")
    parser.add_argument("--advertise-host", default=None, help="IP other nodes should use to reach you (defaults to --host, use your Wi-Fi/hotspot IP)")
    args = parser.parse_args()

    node_id = get_or_create_node_id()
    advertise_host = args.advertise_host or (args.host if args.host != "0.0.0.0" else "127.0.0.1")
    node = MeshNode(node_id, advertise_host, args.port)

    print(f"[MeshOS] Node ID: {node_id}")
    print(f"[MeshOS] Listening on ws://{args.host}:{args.port} (advertising as {node.self_addr})")

    # websocket server for incoming peer connections
    ws_server = await websockets.serve(node.handle_inbound, args.host, args.port)

    # REST API on a different port (port+1) to avoid clashing with the ws server
    rest_port = args.port + 1
    app = make_app(node)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, args.host, rest_port)
    await site.start()
    print(f"[MeshOS] REST API on http://{args.host}:{rest_port}  (try /connection_info)")

    for url in args.connect:
        asyncio.create_task(node.dial(url))

    await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
