import asyncio
import json
import os
import socket
import time
from collections import deque

import websockets

from .config import PEERS_FILE, get_local_ips, load_node_name, save_node_name
from .logger import log


class MeshNode:
    def __init__(self, node_id: str, listen_host: str, listen_port: int, advertise_host: str | None):
        self.id = node_id
        self.listen_host = listen_host
        self.name = load_node_name()
        self.listen_port = listen_port

        if advertise_host:
            self.advertise_ip = advertise_host
        else:
            candidates = get_local_ips()
            self.advertise_ip = candidates[0]

        self.self_addr = f"ws://{self.advertise_ip}:{listen_port}"

        # Direct active connections
        self.connections: dict[str, websockets.WebSocketServerProtocol] = {}
        self.peer_addrs: dict[str, str] = {}

        # Full mesh graph topology
        self.graph: dict[str, dict] = {
            self.id: {
                "addr": self.self_addr,
                "neighbors": [],
                "last_seen": time.time(),
                "name": self.name,
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
        self.sent: list[dict] = []
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
                log(f"Loaded {len(self.persistent_peers)} persistent peers from {PEERS_FILE}")
            except Exception as e:
                log(f"Failed loading {PEERS_FILE}: {e}")

    def save_persistent_peers_to_disk(self):
        """Writes persistent peer list to JSON file."""
        try:
            with open(PEERS_FILE, "w") as f:
                json.dump(sorted(list(self.persistent_peers)), f, indent=2)
        except Exception as e:
            log(f"Failed writing {PEERS_FILE}: {e}")

    def save_persistent_peer(self, url: str):
        """Saves a newly discovered/dialed peer URL to known_peers.json."""
        if not url or url == self.self_addr or url in self.persistent_peers:
            return
        self.persistent_peers.add(url)
        self.save_persistent_peers_to_disk()
        log(f"Saved new persistent peer: {url}")

    def _touch_self(self):
        self.graph[self.id]["neighbors"] = list(self.connections.keys())
        self.graph[self.id]["last_seen"] = time.time()
        self.graph[self.id]["addr"] = self.self_addr
        self.graph[self.id]["name"] = self.name

    def set_name(self, name: str):
        """Rename this node; gossip carries the new name to everyone."""
        self.name = name.strip()[:32] or socket.gethostname()
        save_node_name(self.name)
        self._touch_self()

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
            "self_name": self.name,
            "advertise_ip": self.advertise_ip,
            "graph_version": self.graph_version,
            "direct_peers": list(self.connections.keys()),
            "online_peers": list(self.known_beacons.keys()),
            "graph": self.graph,
        }