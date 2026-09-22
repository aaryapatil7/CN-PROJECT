"""
Global Configuration for Decentralized Multi-Threaded P2P File-Sharing System.

Defines network parameters, Selective Repeat sliding window constants,
chunking settings, timeouts, and simulation controls.
"""

import os
import socket


def get_local_ip() -> str:
    """Detect local LAN IPv4 address of this machine."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Connecting to a LAN IP doesn't actually send packets but selects outgoing interface
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
    except Exception:
        try:
            ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            ip = "127.0.0.1"
    finally:
        s.close()
    return ip


def get_broadcast_addresses() -> list:
    """Return standard and subnet-directed broadcast addresses."""
    addrs = ["<broadcast>", "255.255.255.255"]
    local_ip = get_local_ip()
    if "." in local_ip and local_ip != "127.0.0.1":
        prefix = local_ip.rsplit(".", 1)[0]
        addrs.append(f"{prefix}.255")
    return list(dict.fromkeys(addrs))


# --- Protocol & Transport Parameters ---
MAGIC_COOKIE = 0x5032       # 2-byte magic identifier ('P2')
PROTOCOL_VERSION = 1        # Protocol version number

# Maximum UDP packet size (bytes)
# Typical Ethernet MTU is 1500 bytes. With 20B IP header + 8B UDP header + 16B custom header,
# 1400 byte payload fits in a single unfragmented Ethernet frame.
MAX_PACKET_PAYLOAD = 1400
HEADER_SIZE = 18            # 18-byte fixed header struct (!HBBIIHI)
MAX_PACKET_SIZE = MAX_PACKET_PAYLOAD + HEADER_SIZE

# --- Selective Repeat Sliding Window Constants ---
DEFAULT_WINDOW_SIZE = 8     # Default sliding window size (packets)
DEFAULT_TIMEOUT = 0.8       # Per-packet retransmission timeout (seconds)
MAX_RETRANSMISSIONS = 10    # Maximum retries before dropping a connection / chunk
TIMER_CHECK_INTERVAL = 0.05 # Interval to poll timer queue in sender (seconds)

# --- File & Chunking Parameters ---
DEFAULT_CHUNK_SIZE = 64 * 1024  # 64 KB per file chunk
MAX_CONCURRENT_DOWNLOAD_THREADS = 4

# --- Network & Discovery Defaults ---
DEFAULT_HOST = "0.0.0.0"    # Bind to 0.0.0.0 to listen on all interfaces (LAN & loopback)
DEFAULT_PORT = 5001
DISCOVERY_BROADCAST_PORT = 5001
DISCOVERY_INTERVAL = 1.5    # Peer announcement beacon interval (seconds)
PEER_TIMEOUT = 60.0         # Seconds without beacon before marking peer as inactive (1 min)

# Default known peer IP addresses for multi-computer deployment
KNOWN_PEER_IPS = [
    "10.30.164.22",
    "10.30.164.23",
    "10.30.164.24",
]

# Mapping from IP address to default Peer ID
PEER_IP_MAP = {
    "10.30.164.22": "PEER_A",
    "10.30.164.23": "PEER_B",
    "10.30.164.24": "PEER_C",
}

# Candidate ports to scan for discovery on each IP
LOCAL_SCAN_PORTS = [5001, 5002, 5003, 5004, 5005]

# --- Simulation Defaults ---
DEFAULT_LOSS_PROB = 0.0       # Simulated packet loss probability (0.0 to 1.0)
DEFAULT_CORRUPTION_PROB = 0.0 # Simulated checksum corruption probability

# --- Directory Paths ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SHARED_DIR = os.path.join(BASE_DIR, "shared_files")
DOWNLOADS_DIR = os.path.join(BASE_DIR, "downloads")
LOGS_DIR = os.path.join(BASE_DIR, "logs")

# Ensure required directories exist
for d in [SHARED_DIR, DOWNLOADS_DIR, LOGS_DIR]:
    os.makedirs(d, exist_ok=True)
