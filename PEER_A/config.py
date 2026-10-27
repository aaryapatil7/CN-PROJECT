# =============================================================================
# config.py – Central Configuration File
# =============================================================================
# CHANGE THESE VALUES on each computer before running:
#
#   Computer A: TCP_PORT = 5000, UDP_PORT = 6000
#   Computer B: TCP_PORT = 5001, UDP_PORT = 6001
#   Computer C: TCP_PORT = 5002, UDP_PORT = 6002
#
# Also update KNOWN_PEERS below with the actual IP addresses of all 3 computers.
# =============================================================================

# ---------------------------------------------------------------------------
# NETWORK SETTINGS – Change TCP_PORT and UDP_PORT on every computer
# ---------------------------------------------------------------------------

PEER_NAME = "Peer A"     # Change to Peer A / Peer B / Peer C on each computer
PEER_IP  = "0.0.0.0"   # Listen on all interfaces (do NOT change this)
TCP_PORT = 5000         # This peer's TCP port  (change per computer)
UDP_PORT = 6000         # This peer's UDP port  (change per computer)

# ---------------------------------------------------------------------------
# KNOWN PEERS – Update IP addresses to match your actual LAN IPs
# ---------------------------------------------------------------------------
# List all 3 peers here on EVERY computer.
# Use "ipconfig" on Windows to find each computer's IPv4 address.
#
# Example LAN setup:
#   Computer A: 192.168.1.10
#   Computer B: 192.168.1.11
#   Computer C: 192.168.1.12
#
# For testing on ONE computer, use 127.0.0.1 (loopback) for all IPs.
# ---------------------------------------------------------------------------
KNOWN_PEERS = [
    {
        "name"     : "Peer A",
        "ip"       : "127.0.0.1",   # <-- Change to Computer A's IP
        "tcp_port" : 5000,
        "udp_port" : 6000,
    },
    {
        "name"     : "Peer B",
        "ip"       : "127.0.0.1",   # <-- Change to Computer B's IP
        "tcp_port" : 5001,
        "udp_port" : 6001,
    },
    {
        "name"     : "Peer C",
        "ip"       : "127.0.0.1",   # <-- Change to Computer C's IP
        "tcp_port" : 5002,
        "udp_port" : 6002,
    },
]

# ---------------------------------------------------------------------------
# FILE TRANSFER SETTINGS
# ---------------------------------------------------------------------------

PACKET_SIZE   = 1024          # Bytes per UDP chunk (1 KB)
MIN_DEMO_CHUNKS = 100         # Recommended minimum for the demo file
SHARED_FOLDER = "shared"      # Files this peer offers to other peers
RECEIVED_FOLDER = "received" # Files received from other peers

# ---------------------------------------------------------------------------
# UDP SELECTIVE REPEAT SETTINGS
# ---------------------------------------------------------------------------

WINDOW_SIZE = 8    # Max unacknowledged packets in flight at once
TIMEOUT     = 1    # Seconds before retransmitting an unACK'd packet

# ---------------------------------------------------------------------------
# SIMULATED PACKET LOSS (UDP only)
# ---------------------------------------------------------------------------
# 0.00 = no loss  |  0.10 = 10% loss  |  0.20 = 20% loss
LOSS_RATE = 0.10

