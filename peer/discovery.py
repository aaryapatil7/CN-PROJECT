"""
Decentralized Peer Discovery and Registry Management.

Maintains peer membership and shared file availability using UDP beacons,
local port scanning, and periodic heartbeats.
"""

import time
import socket
import threading
from typing import Dict, List, Tuple, Any, Optional

from protocol.packet import Packet, PacketType
from config import (
    DISCOVERY_INTERVAL,
    PEER_TIMEOUT,
    LOCAL_SCAN_PORTS,
    KNOWN_PEER_IPS,
    get_local_ip,
    get_broadcast_addresses,
)


class PeerDiscovery:
    """
    Decentralized discovery engine using UDP beacons and peer table synchronization.
    Supports multi-device LAN discovery via direct seed pings, broadcast, and instant handshakes.
    """

    def __init__(
        self,
        peer_id: str,
        host: str,
        port: int,
        get_shared_files_fn,
        sock: Optional[socket.socket] = None,
        log_callback: Optional[Any] = None,
        known_peer_ips: Optional[List[str]] = None,
        candidate_ports: Optional[List[int]] = None,
    ):
        self.peer_id = peer_id
        self.host = host
        self.port = port
        self.sock = sock
        self.get_shared_files_fn = get_shared_files_fn
        self.log_callback = log_callback or (lambda msg: None)

        self.known_peer_ips = list(known_peer_ips) if known_peer_ips is not None else list(KNOWN_PEER_IPS)
        self.candidate_ports = list(candidate_ports) if candidate_ports is not None else list(LOCAL_SCAN_PORTS)
        self.local_ip = get_local_ip()

        # Peer Table: peer_id -> {"ip": str, "port": int, "shared_files": list, "last_seen": float}
        self.peers: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()
        self.stop_event = threading.Event()

        # Dedicated discovery socket with broadcast enabled (fallback if sock not provided)
        self.discovery_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.discovery_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SIO_UDP_CONNRESET"):
            try:
                self.discovery_sock.ioctl(socket.SIO_UDP_CONNRESET, False)
            except Exception:
                pass
        try:
            self.discovery_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        except OSError:
            pass

        if self.sock:
            try:
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            except OSError:
                pass

    def start(self) -> None:
        """Start discovery beacon thread and listener."""
        self.beacon_thread = threading.Thread(target=self._beacon_loop, daemon=True)
        self.prune_thread = threading.Thread(target=self._prune_loop, daemon=True)
        self.beacon_thread.start()
        self.prune_thread.start()

    def stop(self) -> None:
        """Stop discovery loops and close socket."""
        self.stop_event.set()
        try:
            self.discovery_sock.close()
        except Exception:
            pass

    def update_peer(self, peer_id: str, ip: str, port: int, shared_files: List[str]) -> None:
        """Record or update an active peer in the registry."""
        if peer_id == self.peer_id:
            return  # Ignore self

        with self.lock:
            is_new = peer_id not in self.peers
            self.peers[peer_id] = {
                "ip": ip,
                "port": port,
                "shared_files": shared_files,
                "last_seen": time.time(),
            }
            if is_new:
                self.log_callback(f"[DISCOVERY] Discovered new peer: {peer_id} at {ip}:{port} (Files: {shared_files})")
                # Immediate handshake reply so the newly discovered peer registers us instantly
                try:
                    reply_pkt = Packet.create_discovery(self.peer_id, self.port, self.get_shared_files_fn())
                    s = self.sock or self.discovery_sock
                    s.sendto(reply_pkt.encode(), (ip, port))
                except Exception:
                    pass

    def send_discovery_pulse(self) -> None:
        """Immediately dispatch discovery beacons to all seeds, known peers, and broadcast."""
        shared_files = self.get_shared_files_fn()
        disc_pkt = Packet.create_discovery(
            peer_id=self.peer_id,
            port=self.port,
            shared_files=shared_files,
        )
        raw = disc_pkt.encode()
        s = self.sock or self.discovery_sock

        # Unicast to seeds & localhost
        target_ips = set(self.known_peer_ips)
        target_ips.add("127.0.0.1")
        for target_ip in target_ips:
            for target_port in self.candidate_ports:
                if (target_ip in ("127.0.0.1", "localhost", self.local_ip)) and target_port == self.port:
                    continue
                try:
                    s.sendto(raw, (target_ip, target_port))
                except OSError:
                    pass

        # Broadcast addresses
        for b_ip in get_broadcast_addresses():
            for target_port in self.candidate_ports:
                try:
                    s.sendto(raw, (b_ip, target_port))
                except OSError:
                    pass

    def _beacon_loop(self) -> None:
        """Periodically broadcast discovery announcements to find other peers."""
        while not self.stop_event.is_set():
            shared_files = self.get_shared_files_fn()
            disc_pkt = Packet.create_discovery(
                peer_id=self.peer_id,
                port=self.port,
                shared_files=shared_files,
            )
            raw = disc_pkt.encode()
            s = self.sock or self.discovery_sock

            # 1. Send directly to configured known peer IPs across candidate ports (LAN seeds)
            target_ips = set(self.known_peer_ips)
            target_ips.add("127.0.0.1")  # Always include loopback for local tests

            for target_ip in target_ips:
                for target_port in self.candidate_ports:
                    if (target_ip in ("127.0.0.1", "localhost", self.local_ip)) and target_port == self.port:
                        continue
                    try:
                        s.sendto(raw, (target_ip, target_port))
                    except OSError:
                        pass

            # 2. Also ping any already-known peers directly
            with self.lock:
                known_endpoints = [(p["ip"], p["port"]) for p in self.peers.values()]
            for ip, pport in known_endpoints:
                try:
                    s.sendto(raw, (ip, pport))
                except OSError:
                    pass

            # 3. Send broadcast beacon to standard & subnet broadcast addresses
            for b_ip in get_broadcast_addresses():
                for target_port in self.candidate_ports:
                    try:
                        s.sendto(raw, (b_ip, target_port))
                    except OSError:
                        pass

            self.stop_event.wait(DISCOVERY_INTERVAL)

    def _prune_loop(self) -> None:
        """Remove peers that have not sent a beacon within PEER_TIMEOUT."""
        while not self.stop_event.is_set():
            now = time.time()
            with self.lock:
                expired = [
                    pid for pid, info in self.peers.items()
                    if now - info["last_seen"] > PEER_TIMEOUT
                ]
                for pid in expired:
                    del self.peers[pid]
                    self.log_callback(f"[DISCOVERY] Peer {pid} timed out and removed from network table.")
            self.stop_event.wait(5.0)

    def get_active_peers(self) -> Dict[str, Dict[str, Any]]:
        """Return a copy of the active peer table."""
        with self.lock:
            return dict(self.peers)

    def find_peers_with_file(self, file_name: str) -> List[Tuple[str, str, int]]:
        """
        Find all active peers holding a copy of the requested file.

        Returns:
            List of tuples: (peer_id, ip, port)
        """
        candidates = []
        with self.lock:
            for pid, info in self.peers.items():
                if file_name in info.get("shared_files", []):
                    candidates.append((pid, info["ip"], info["port"]))
        return candidates

    def get_all_network_files(self) -> Dict[str, List[str]]:
        """
        Aggregate all available files in the network.

        Returns:
            Dict mapping file_name -> list of peer_ids holding it.
        """
        file_map: Dict[str, List[str]] = {}
        with self.lock:
            for pid, info in self.peers.items():
                for f in info.get("shared_files", []):
                    if f not in file_map:
                        file_map[f] = []
                    file_map[f].append(pid)
        return file_map
