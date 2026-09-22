"""
Decentralized P2P Node implementation.

Handles UDP socket listener, request dispatching, file serving via Selective Repeat,
concurrent downloading from multiple peers, and discovery integration.
"""

import os
import time
import socket
import threading
from typing import Dict, List, Tuple, Any, Optional, Callable

from protocol.packet import Packet, PacketType
from protocol.selective_repeat import SelectiveRepeatSession
from file_manager.chunk_manager import ChunkManager
from file_manager.file_reassembler import FileReassembler
from .discovery import PeerDiscovery
from .downloader import MultiPeerDownloader
from .stats import NetworkStats
from config import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    DEFAULT_WINDOW_SIZE,
    DEFAULT_TIMEOUT,
    DEFAULT_LOSS_PROB,
    DEFAULT_CORRUPTION_PROB,
    MAX_PACKET_SIZE,
)


class PeerNode:
    """
    Core Peer Node representation.
    """

    def __init__(
        self,
        peer_id: str,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        shared_dir: str = "shared_files",
        downloads_dir: str = "downloads",
        window_size: int = DEFAULT_WINDOW_SIZE,
        timeout: float = DEFAULT_TIMEOUT,
        loss_prob: float = DEFAULT_LOSS_PROB,
        corruption_prob: float = DEFAULT_CORRUPTION_PROB,
        log_callback: Optional[Callable[[str], None]] = None,
    ):
        self.peer_id = peer_id
        self.host = host
        self.port = port
        self.window_size = window_size
        self.timeout = timeout
        self.loss_prob = loss_prob
        self.corruption_prob = corruption_prob
        self.log_callback = log_callback or (lambda msg: None)

        # Network Statistics Tracker
        self.stats = NetworkStats()

        # Storage & Chunk Managers
        self.chunk_manager = ChunkManager(shared_dir)
        self.reassembler = FileReassembler(downloads_dir)

        # Main UDP socket for all P2P file transfers and signaling
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if os.name == "nt" and hasattr(socket, "SIO_UDP_CONNRESET"):
            try:
                self.sock.ioctl(socket.SIO_UDP_CONNRESET, False)
            except Exception:
                pass
        self.sock.bind((self.host, self.port))

        # Selective Repeat Session Coordinator (for serving chunks)
        self.sr_session = SelectiveRepeatSession(
            sock=self.sock,
            window_size=self.window_size,
            timeout=self.timeout,
            loss_prob=self.loss_prob,
            corruption_prob=self.corruption_prob,
            log_callback=self.log_callback,
            stats_callback=self.stats.record,
        )

        # Multi-Peer Downloader
        self.downloader = MultiPeerDownloader(
            sock=self.sock,
            reassembler=self.reassembler,
            window_size=self.window_size,
            log_callback=self.log_callback,
            stats_callback=self.stats.record,
        )

        # Decentralized Discovery Engine
        self.discovery = PeerDiscovery(
            peer_id=self.peer_id,
            host=self.host,
            port=self.port,
            get_shared_files_fn=self.chunk_manager.list_shared_files,
            sock=self.sock,
            log_callback=self.log_callback,
        )

        self.stop_event = threading.Event()
        self.metadata_responses: Dict[str, Dict[str, Any]] = {}
        self.metadata_waiters: Dict[str, threading.Event] = {}
        self.metadata_lock = threading.Lock()

    def set_loss_probability(self, loss_prob: float) -> None:
        """Update simulated packet loss probability dynamically."""
        self.loss_prob = max(0.0, min(1.0, loss_prob))
        self.sr_session.loss_prob = self.loss_prob
        self.log_callback(f"[CONFIG] Packet loss probability updated to {self.loss_prob * 100:.1f}%")

    def set_window_size(self, window_size: int) -> None:
        """Update sliding window size dynamically."""
        self.window_size = max(1, window_size)
        self.sr_session.window_size = self.window_size
        self.downloader.window_size = self.window_size
        self.log_callback(f"[CONFIG] Sliding window size updated to {self.window_size}")

    def start(self) -> None:
        """Start listening for incoming packets and launch discovery beacon."""
        self.recv_thread = threading.Thread(target=self._socket_listener, daemon=True)
        self.recv_thread.start()
        self.discovery.start()
        self.log_callback(f"[PEER] Peer '{self.peer_id}' listening on UDP {self.host}:{self.port}")

    def stop(self) -> None:
        """Stop peer node services and release sockets."""
        self.stop_event.set()
        self.discovery.stop()
        try:
            self.sock.close()
        except Exception:
            pass

    def _socket_listener(self) -> None:
        """Main UDP datagram receiver loop."""
        self.sock.settimeout(0.5)
        while not self.stop_event.is_set():
            try:
                raw_bytes, src_addr = self.sock.recvfrom(65535)
            except (socket.timeout, ConnectionResetError):
                continue
            except OSError as e:
                if getattr(e, "winerror", None) == 10054 or getattr(e, "errno", None) == 10054:
                    continue
                break

            try:
                pkt = Packet.decode(raw_bytes)
            except ValueError as e:
                self.stats.record("packets_corrupted", 1)
                self.log_callback(f"[CORRUPT] Packet decode error from {src_addr}: {e}")
                continue

            self._dispatch_packet(pkt, src_addr)

    def _dispatch_packet(self, pkt: Packet, src_addr: Tuple[str, int]) -> None:
        """Route incoming decoded packet to appropriate handler."""
        # 1. Peer Discovery Announcement
        if pkt.pkt_type == PacketType.DISCOVERY:
            try:
                pinfo = pkt.payload_as_json()
                self.discovery.update_peer(
                    peer_id=pinfo["peer_id"],
                    ip=src_addr[0],
                    port=pinfo["port"],
                    shared_files=pinfo.get("shared_files", []),
                )
            except Exception as e:
                self.log_callback(f"[ERROR] Failed to parse discovery payload: {e}")

        # 2. File Metadata Request
        elif pkt.pkt_type == PacketType.METADATA_REQ:
            try:
                req_data = pkt.payload_as_json()
                file_name = req_data.get("file_name", "")
                manifest = self.chunk_manager.get_file_metadata(file_name)
                if manifest:
                    resp_pkt = Packet.create_metadata_resp(manifest)
                else:
                    resp_pkt = Packet.create_error(f"File not found: {file_name}")
                self.sock.sendto(resp_pkt.encode(), src_addr)
            except Exception as e:
                self.log_callback(f"[ERROR] Error handling metadata request: {e}")

        # 3. File Metadata Response
        elif pkt.pkt_type == PacketType.METADATA_RESP:
            try:
                manifest = pkt.payload_as_json()
                fname = manifest.get("file_name", "")
                with self.metadata_lock:
                    self.metadata_responses[fname] = manifest
                    if fname in self.metadata_waiters:
                        self.metadata_waiters[fname].set()
            except Exception as e:
                self.log_callback(f"[ERROR] Error handling metadata response: {e}")

        # 4. Chunk Download Request (We are the Sender / Provider)
        elif pkt.pkt_type == PacketType.REQUEST:
            try:
                req_data = pkt.payload_as_json()
                file_name = req_data.get("file_name", "")
                chunk_id = req_data.get("chunk_id", 0)
                chunk_bytes = self.chunk_manager.read_chunk(file_name, chunk_id)
                if chunk_bytes is not None:
                    # Serve the chunk using Selective Repeat in a worker thread
                    t = threading.Thread(
                        target=self.sr_session.send_chunk_data,
                        args=(src_addr, chunk_id, chunk_bytes, True),
                        daemon=True,
                    )
                    t.start()
                else:
                    err_pkt = Packet.create_error(f"Chunk {chunk_id} not available for {file_name}", chunk_id)
                    self.sock.sendto(err_pkt.encode(), src_addr)
            except Exception as e:
                self.log_callback(f"[ERROR] Error processing chunk request: {e}")

        # 5. Selective Repeat ACKs (Sender received ACK)
        elif pkt.pkt_type == PacketType.ACK:
            self.sr_session.route_incoming_packet(pkt, src_addr)

        # 6. DATA or FIN packets (Downloader received chunk data)
        elif pkt.pkt_type in (PacketType.DATA, PacketType.FIN):
            handled = self.downloader.route_packet(pkt, src_addr)
            if not handled:
                self.sr_session.route_incoming_packet(pkt, src_addr)

    def fetch_file_metadata(self, file_name: str, target_peer: Tuple[str, str, int], max_retries: int = 3, timeout: float = 2.0) -> Optional[Dict[str, Any]]:
        """
        Request metadata manifest for a file from a remote peer with automatic retry.
        """
        _, p_ip, p_port = target_peer
        dest_addr = (p_ip, p_port)

        for attempt in range(1, max_retries + 1):
            event = threading.Event()
            with self.metadata_lock:
                self.metadata_waiters[file_name] = event
                self.metadata_responses.pop(file_name, None)

            req_pkt = Packet.create_metadata_req(file_name)
            try:
                self.sock.sendto(req_pkt.encode(), dest_addr)
            except socket.error as e:
                self.log_callback(f"[ERROR] Failed to send metadata request: {e}")
                return None

            if event.wait(timeout=timeout):
                with self.metadata_lock:
                    resp = self.metadata_responses.get(file_name)
                    if resp:
                        return resp

            if attempt < max_retries:
                self.log_callback(f"[WARN] Metadata request timeout for '{file_name}' from {dest_addr}. Retrying ({attempt}/{max_retries})...")
                time.sleep(0.3)

        return None

    def download_file(self, file_name: str) -> Tuple[bool, str, str]:
        """
        High-level method to download a file from active peers possessing it.
        """
        # 1. Discover peers with this file
        source_peers = self.discovery.find_peers_with_file(file_name)
        if not source_peers:
            # Trigger immediate local scan beacon pulse
            disc_pkt = Packet.create_discovery(self.peer_id, self.port, self.chunk_manager.list_shared_files())
            for p in range(5001, 5011):
                if p != self.port:
                    try:
                        self.sock.sendto(disc_pkt.encode(), ("127.0.0.1", p))
                    except Exception:
                        pass
            time.sleep(0.8)
            source_peers = self.discovery.find_peers_with_file(file_name)

        if not source_peers:
            return False, f"File '{file_name}' not found on any active peers. Make sure Peer A and Peer C are running.", ""

        # 2. Fetch manifest from the first available peer
        manifest = None
        for peer_info in source_peers:
            manifest = self.fetch_file_metadata(file_name, peer_info)
            if manifest:
                break

        if not manifest:
            return False, f"Could not retrieve file manifest for '{file_name}'.", ""

        # 3. Start download and record metrics
        self.stats.mark_start()
        success, msg, final_path = self.downloader.download_file(file_name, manifest, source_peers)
        self.stats.mark_end()

        # If download succeeded, automatically share the new file!
        if success:
            self.chunk_manager.refresh_shared_files()

        return success, msg, final_path
