import os
import socket
import uuid

ID_FILE = ".mesh_node_id"
PEERS_FILE = "known_peers.json"
NAME_FILE = ".mesh_node_name"
UDP_PORT = 3848          # discovery port
BEACON_INTERVAL = 3.0    # seconds
PEER_TIMEOUT = 15.0      # seconds without beacon → consider offline
AUTO_RECONNECT_INTERVAL = 8.0  # auto-reconnect check cycle (seconds)


def get_or_create_node_id() -> str:
    if os.path.exists(ID_FILE):
        with open(ID_FILE) as f:
            return f.read().strip()
    nid = uuid.uuid4().hex[:12]
    with open(ID_FILE, "w") as f:
        f.write(nid)
    return nid


def get_local_ips() -> list[str]:
    """Return non-loopback IPv4 addresses (Wi-Fi, LAN, Bluetooth PAN, etc.), prioritizing real LAN IPs over APIPA/WSL."""
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


def load_node_name() -> str:
    """Friendly display name shown to other nodes (defaults to hostname)."""
    try:
        if os.path.exists(NAME_FILE):
            with open(NAME_FILE, encoding="utf-8") as f:
                name = f.read().strip()
                if name:
                    return name
    except Exception:
        pass
    return socket.gethostname()


def save_node_name(name: str):
    with open(NAME_FILE, "w", encoding="utf-8") as f:
        f.write(name)