#!/usr/bin/env python3
"""
MeshOS - Offline Mesh Node Entry Point
======================================
Usage:
    python server.py --port 8765
    python server.py --port 8770
"""

import argparse
import asyncio
import sys
import websockets
from aiohttp import web

from mesh import (
    MeshNode,
    UDP_PORT,
    get_local_ips,
    get_or_create_node_id,
    log,
    make_app,
    setup_logger,
    start_network_services,
)
from mesh.network import NetworkManager


async def main():
    parser = argparse.ArgumentParser(description="MeshOS - Auto-discovering mesh node")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--advertise-host", default=None,
                        help="IP that other nodes should use to reach you")
    parser.add_argument("--connect", action="append", default=[],
                        help="Optional manual seed: ws://ip:port")
    parser.add_argument("--name", default=None, help="Display name shown to other nodes")
    parser.add_argument("--log-file", default="meshos.log",
                        help="Log file name (default: meshos.log)")
    args = parser.parse_args()

    setup_logger(args.log_file)

    node_id = get_or_create_node_id()
    node = MeshNode(node_id, args.host, args.port, args.advertise_host)
    if args.name:
        node.set_name(args.name)
    net = NetworkManager(node)

    all_ips = get_local_ips()
    print("=" * 60)
    print(f"  MeshOS Node ID     : {node.id}")
    print(f"  Display name       : {node.name}")
    print(f"  Open UI            : http://{node.advertise_ip}:{args.port + 1}/")
    print(f"  WebSocket          : {node.self_addr}")
    print(f"  REST API           : http://{node.advertise_ip}:{args.port + 1}")
    print(f"  Discovery (UDP)    : port {UDP_PORT}")
    print(f"  Detected IPs       : {', '.join(all_ips)}")
    print(f"  Log File           : {args.log_file}")
    print("=" * 60)

    # WebSocket server
    ws_server = await websockets.serve(
        net.ws_handler,
        args.host,
        args.port,
        process_request=net.process_http_request,
        ping_interval=20,
        ping_timeout=20,
    )

    # REST API on port + 1
    app = make_app(node, net)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, args.host, args.port + 1)
    await site.start()

    # Start network tasks (UDP broadcast, UDP listener, watchdog, auto-reconnect)
    start_network_services(node, net)

    # Optional manual seeds
    for url in args.connect:
        node.save_persistent_peer(url)
        asyncio.create_task(net.dial(url))

    log("Running... (auto-discovery & persistent auto-reconnect active)")
    await asyncio.Future()  # Run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[MeshOS] Shutting down")