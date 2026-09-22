"""
Multi-Threaded Concurrent Chunk Downloader.

Downloads chunks concurrently from multiple peers over UDP using
the Selective Repeat protocol, handles failover/retries, and triggers file reassembly.
"""

import time
import socket
import queue
import threading
from typing import List, Tuple, Dict, Any, Optional, Callable

from protocol.packet import Packet, PacketType
from protocol.receiver import SRReceiver
from file_manager.file_reassembler import FileReassembler
from config import (
    DEFAULT_WINDOW_SIZE,
    MAX_CONCURRENT_DOWNLOAD_THREADS,
)


class MultiPeerDownloader:
    """
    Coordinates concurrent multi-threaded chunk downloads across multiple peer sources.
    """

    def __init__(
        self,
        sock: socket.socket,
        reassembler: FileReassembler,
        window_size: int = DEFAULT_WINDOW_SIZE,
        max_threads: int = MAX_CONCURRENT_DOWNLOAD_THREADS,
        log_callback: Optional[Callable[[str], None]] = None,
        stats_callback: Optional[Callable[[str, int], None]] = None,
        progress_callback: Optional[Callable[[int, int, float], None]] = None,
    ):
        self.sock = sock
        self.reassembler = reassembler
        self.window_size = window_size
        self.max_threads = max_threads
        self.log_callback = log_callback or (lambda msg: None)
        self.stats_callback = stats_callback or (lambda key, val: None)
        self.progress_callback = progress_callback or (lambda done, total, speed: None)

        # Active receiver registry for routing incoming packets: (sender_addr, chunk_id) -> SRReceiver
        self.active_receivers: Dict[Tuple[Tuple[str, int], int], SRReceiver] = {}
        self.receiver_lock = threading.Lock()

    def route_packet(self, pkt: Packet, src_addr: Tuple[str, int]) -> bool:
        """
        Route an incoming DATA or FIN packet to its active SRReceiver worker.
        """
        if pkt.pkt_type in (PacketType.DATA, PacketType.FIN):
            key = (src_addr, pkt.chunk_id)
            with self.receiver_lock:
                receiver = self.active_receivers.get(key)
            if receiver:
                receiver.handle_packet(pkt)
                return True
        return False

    def download_file(
        self,
        file_name: str,
        manifest: Dict[str, Any],
        source_peers: List[Tuple[str, str, int]],  # List of (peer_id, ip, port)
    ) -> Tuple[bool, str, str]:
        """
        Execute concurrent multi-peer download of the target file.

        Args:
            file_name: Target file name.
            manifest: File metadata manifest containing total_chunks, file_sha256, etc.
            source_peers: Available peers possessing the file.

        Returns:
            Tuple of (success: bool, message: str, final_path: str).
        """
        if not source_peers:
            return False, "No active peers available for this file.", ""

        total_chunks = manifest["total_chunks"]
        expected_sha256 = manifest["file_sha256"]
        file_size = manifest.get("file_size", 0)

        # Check for resume support (missing chunks)
        missing_chunks = self.reassembler.get_missing_chunks(file_name, total_chunks)
        downloaded_count = total_chunks - len(missing_chunks)

        if not missing_chunks:
            self.log_callback(f"[RESUME] All {total_chunks} chunks already present locally. Reassembling...")
            return self.reassembler.reassemble_file(file_name, total_chunks, expected_sha256)

        self.log_callback(
            f"[DOWNLOAD] Starting download: '{file_name}' ({file_size / 1024:.1f} KB, "
            f"{total_chunks} chunks). Missing chunks to fetch: {len(missing_chunks)} "
            f"across {len(source_peers)} peers."
        )

        # Work queue of chunk IDs to download
        chunk_queue: queue.Queue = queue.Queue()
        for cid in missing_chunks:
            chunk_queue.put(cid)

        completed_chunks_lock = threading.Lock()
        completed_count = [downloaded_count]
        failed_chunks = []
        start_time = time.time()
        num_workers = min(self.max_threads, len(missing_chunks))

        def worker_loop(worker_id: int):
            peer_idx = worker_id % len(source_peers)
            while not chunk_queue.empty():
                try:
                    cid = chunk_queue.get_nowait()
                except queue.Empty:
                    break

                target_peer = source_peers[peer_idx % len(source_peers)]
                peer_idx += 1
                peer_label, p_ip, p_port = target_peer
                dest_addr = (p_ip, p_port)

                self.log_callback(f"[THREAD-{worker_id}] Requesting Chunk {cid} from {peer_label} ({p_ip}:{p_port})")

                # Setup SRReceiver for this chunk
                receiver = SRReceiver(
                    sock=self.sock,
                    sender_addr=dest_addr,
                    chunk_id=cid,
                    window_size=self.window_size,
                    log_callback=self.log_callback,
                    stats_callback=self.stats_callback,
                )

                key = (dest_addr, cid)
                with self.receiver_lock:
                    self.active_receivers[key] = receiver

                # Send CHUNK REQUEST packet to peer
                req_pkt = Packet.create_request(file_name=file_name, chunk_id=cid)
                try:
                    self.sock.sendto(req_pkt.encode(), dest_addr)
                    self.stats_callback("packets_sent", 1)
                except socket.error as e:
                    self.log_callback(f"[ERROR] Failed to send request for Chunk {cid}: {e}")

                # Wait for chunk transfer completion
                success = receiver.finished.wait(timeout=20.0)

                with self.receiver_lock:
                    self.active_receivers.pop(key, None)

                if success:
                    chunk_data = receiver.get_assembled_data()
                    self.reassembler.save_chunk(file_name, cid, chunk_data)
                    self.stats_callback("bytes_transferred", len(chunk_data))

                    with completed_chunks_lock:
                        completed_count[0] += 1
                        current_done = completed_count[0]

                    elapsed = max(0.001, time.time() - start_time)
                    speed_kbps = (current_done * manifest.get("chunk_size", 65536) / 1024) / elapsed
                    self.progress_callback(current_done, total_chunks, speed_kbps)
                    self.log_callback(f"[PROGRESS] Chunk {cid}/{total_chunks - 1} completed ({current_done}/{total_chunks})")
                    chunk_queue.task_done()
                else:
                    self.log_callback(f"[WARN] Chunk {cid} failed or timed out from {peer_label}. Re-queueing...")
                    chunk_queue.task_done()
                    # Re-queue with alternate peer
                    chunk_queue.put(cid)
                    time.sleep(0.5)

        threads = []
        for i in range(num_workers):
            t = threading.Thread(target=worker_loop, args=(i + 1,), daemon=True)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Check if all chunks were received
        still_missing = self.reassembler.get_missing_chunks(file_name, total_chunks)
        if still_missing:
            return False, f"Download failed: missing {len(still_missing)} chunks after retries.", ""

        # Reassemble the file and verify SHA-256
        self.log_callback(f"[REASSEMBLER] Reassembling '{file_name}' from {total_chunks} verified chunks...")
        return self.reassembler.reassemble_file(file_name, total_chunks, expected_sha256)
