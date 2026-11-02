import asyncio
import json
import os
import time
from aiohttp import web

from .network import NetworkManager
from .node import MeshNode


def make_app(node: MeshNode, net: NetworkManager):
    app = web.Application()

    async def connection_info(request):
        return web.json_response(node.snapshot())

    async def peers(request):
        return web.json_response({
            "direct": node.peer_addrs,
            "known_beacons": list(node.known_beacons.keys()),
            "persistent_peers": list(node.persistent_peers),
        })

    async def connect_peer(request):
        data = await request.json()
        target_url = data.get("url")
        if not target_url:
            return web.json_response({"error": "Missing 'url'"}, status=400)
        node.save_persistent_peer(target_url)
        asyncio.create_task(net.dial(target_url))
        return web.json_response({"status": "dialing", "target": target_url})

    async def send_msg(request):
        data = await request.json()
        dest = data.get("dest")
        msg = data.get("msg")

        if not dest or not msg:
            return web.json_response({"error": "Missing 'dest' or 'msg'"}, status=400)

        envelope = {
            "type": "MSG",
            "src": node.id,
            "dest": dest,
            "msg": msg,
            "path": [node.id],
            "ts": time.time(),
        }

        if dest == node.id:
            node.inbox.append({"from": node.id, "msg": msg, "ts": time.time(), "path": [node.id]})
            node.sent.append({"to": dest, "msg": msg, "ts": time.time()})
            return web.json_response({"status": "delivered_locally"})

        next_hop = node.bfs_next_hop(dest)
        if not next_hop or next_hop not in node.connections:
            return web.json_response({"error": f"No route to destination '{dest}'"}, status=404)

        try:
            await node.connections[next_hop].send(json.dumps(envelope))
            node.sent.append({"to": dest, "msg": msg, "ts": time.time()})
            return web.json_response({"status": "sent", "next_hop": next_hop, "dest": dest})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def index(request):
        return web.FileResponse(os.path.join(os.path.dirname(__file__), "ui.html"))

    async def set_name(request):
        data = await request.json()
        name = (data.get("name") or "").strip()
        if not name:
            return web.json_response({"error": "Missing 'name'"}, status=400)
        node.set_name(name)
        await net.broadcast_graph()
        return web.json_response({"status": "ok", "name": node.name})

    async def get_sent(request):
        return web.json_response(node.sent)

    async def get_inbox(request):
        return web.json_response(node.inbox)

    async def refresh_route(request):
        res = await net.refresh_connections()
        return web.json_response(res)

    app.router.add_get("/", index)
    app.router.add_post("/name", set_name)
    app.router.add_get("/sent", get_sent)
    app.router.add_get("/status", connection_info)
    app.router.add_get("/connection_info", connection_info)
    app.router.add_get("/peers", peers)
    app.router.add_post("/connect", connect_peer)
    app.router.add_post("/send", send_msg)
    app.router.add_get("/inbox", get_inbox)
    app.router.add_route("*", "/refresh", refresh_route)
    return app