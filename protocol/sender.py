"""
Selective Repeat Sliding Window Sender implementation over UDP.

Provides reliable data transmission using per-packet timers, selective
retransmission (retransmitting ONLY timed-out unACKed packets), sliding send
window, and simulated packet loss.
"""

import time
import socket
import random
import threading
from typing import Dict, Set, List, Optional, Callable

from .packet import Packet, PacketType
from config import (
    DEFAULT_WINDOW_SIZE,
    DEFAULT_TIMEOUT,
    MAX_PACKET_PAYLOAD,
    MAX_RETRANSMISSIONS,
    TIMER_CHECK_INTERVAL,
)


class SRSender:
    """
    Selective Repeat Sender.

    Transmits a chunk of data sliced into packets over UDP with:
    - Sliding window control [send_base, send_base + window_size - 1]
    - Individual timer per packet
    - Selective retransmission upon timeout
    - Window advancement upon receiving ACK for send_base
    """

    def __init__(
        self,
        sock: socket.socket,
        dest_addr: tuple[str, int],
        chunk_id: int,
        data: bytes,
        window_size: int = DEFAULT_WINDOW_SIZE,
        timeout: float = DEFAULT_TIMEOUT,
        loss_prob: float = 0.0,
        corruption_prob: float = 0.0,
        log_callback: Optional[Callable[[str], None]] = None,
        stats_callback: Optional[Callable[[str, int], None]] = None,
    ):
        self.sock = sock
        self.dest_addr = dest_addr
        self.chunk_id = chunk_id
        self.data = data
        self.window_size = window_size
        self.timeout = timeout
        self.loss_prob = loss_prob
        self.corruption_prob = corruption_prob
        self.log_callback = log_callback or (lambda msg: None)
        self.stats_callback = stats_callback or (lambda key, val: None)

        # Slice data into packets
        self.packets: List[Packet] = self._prepare_packets()
        self.total_packets = len(self.packets)

        # Sliding window state
        self.send_base = 0
        self.next_seq_num = 0
        self.acked_seqs: Set[int] = set()
        self.timers: Dict[int, float] = {}
        self.retransmit_counts: Dict[int, int] = {}

        # Thread synchronization
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.finished = threading.Event()
        self.success = False

    def _log(self, message: str) -> None:
        self.log_callback(message)

    def _record_stat(self, key: str, val: int = 1) -> None:
        self.stats_callback(key, val)

    def _prepare_packets(self) -> List[Packet]:
        """Split chunk data into application-layer DATA packets."""
        packets = []
        if not self.data:
            # Handle 0-byte chunk edge case
            packets.append(Packet.create_data(seq_num=0, chunk_id=self.chunk_id, payload=b"", is_fin=True))
            return packets

        offset = 0
        seq = 0
        while offset < len(self.data):
            end = min(offset + MAX_PACKET_PAYLOAD, len(self.data))
            payload = self.data[offset:end]
            is_fin = (end == len(self.data))
            pkt = Packet.create_data(seq_num=seq, chunk_id=self.chunk_id, payload=payload, is_fin=is_fin)
            packets.append(pkt)
            seq += 1
            offset = end
        return packets

    def handle_ack(self, ack_pkt: Packet) -> None:
        """
        Process an incoming Selective Repeat ACK packet.
        Marks the packet as acknowledged, stops its timer, and slides the window forward.
        """
        if ack_pkt.pkt_type != PacketType.ACK or ack_pkt.chunk_id != self.chunk_id:
            return

        seq = ack_pkt.seq_num
        with self.lock:
            if seq >= self.total_packets:
                return

            self._record_stat("acks_received", 1)
            self._log(f"[ACK] Received ACK={seq} for Chunk {self.chunk_id}")

            if seq not in self.acked_seqs:
                self.acked_seqs.add(seq)
                if seq in self.timers:
                    del self.timers[seq]

                # Check if this ACK allows sliding the send window
                if seq == self.send_base:
                    old_base = self.send_base
                    while self.send_base < self.total_packets and self.send_base in self.acked_seqs:
                        self.send_base += 1
                    self._log(
                        f"[WINDOW] Send window advanced from {old_base} to {self.send_base} "
                        f"[{self.send_base}..{min(self.send_base + self.window_size - 1, self.total_packets - 1)}]"
                    )

            # Check if all packets in this chunk are acknowledged
            if len(self.acked_seqs) == self.total_packets:
                self.success = True
                self.finished.set()

    def _send_packet(self, pkt: Packet, is_retransmit: bool = False) -> None:
        """Transmit packet to destination with simulated packet loss/corruption."""
        # Simulated packet loss
        if self.loss_prob > 0.0 and random.random() < self.loss_prob:
            self._record_stat("packets_lost", 1)
            self._log(
                f"[LOSS] Simulated loss of DATA Seq={pkt.seq_num} Chunk={pkt.chunk_id} "
                f"{'(Retransmission)' if is_retransmit else ''}"
            )
            return

        raw_bytes = pkt.encode()

        # Simulated packet corruption
        if self.corruption_prob > 0.0 and random.random() < self.corruption_prob:
            from .checksum import corrupt_bytes
            raw_bytes = corrupt_bytes(raw_bytes)
            self._record_stat("packets_corrupted", 1)
            self._log(f"[CORRUPT] Simulated corruption of DATA Seq={pkt.seq_num} Chunk={pkt.chunk_id}")

        try:
            self.sock.sendto(raw_bytes, self.dest_addr)
            if is_retransmit:
                self._record_stat("packets_retransmitted", 1)
                self._log(f"[RETRANSMIT] Sent DATA Seq={pkt.seq_num} Chunk={pkt.chunk_id}")
            else:
                self._record_stat("packets_sent", 1)
                self._log(f"[DATA] Sent DATA Seq={pkt.seq_num} Chunk={pkt.chunk_id} ({pkt.payload_len} bytes)")
        except socket.error as e:
            self._log(f"[ERROR] Socket sendto failed for Seq={pkt.seq_num}: {e}")

    def send_chunk(self, blocking: bool = True) -> bool:
        """
        Execute the Selective Repeat sender transmission loop.
        Sends packets within sliding window and handles per-packet timeouts.
        """
        if self.total_packets == 0:
            return True

        self._log(
            f"[SENDER] Starting Selective Repeat transfer: Chunk {self.chunk_id} "
            f"({len(self.data)} bytes, {self.total_packets} packets, Window={self.window_size})"
        )

        while not self.stop_event.is_set() and not self.finished.is_set():
            with self.lock:
                # 1. Transmit new packets that fit within sliding window [send_base, send_base + window_size - 1]
                while (
                    self.next_seq_num < self.send_base + self.window_size
                    and self.next_seq_num < self.total_packets
                ):
                    pkt = self.packets[self.next_seq_num]
                    self.timers[self.next_seq_num] = time.time()
                    self.retransmit_counts[self.next_seq_num] = 0
                    self._send_packet(pkt, is_retransmit=False)
                    self.next_seq_num += 1

                # 2. Check individual timers for unACKed packets currently within window
                now = time.time()
                for seq in range(self.send_base, self.next_seq_num):
                    if seq not in self.acked_seqs and seq in self.timers:
                        if now - self.timers[seq] > self.timeout:
                            # Packet has timed out - SELECTIVE RETRANSMIT ONLY THIS PACKET
                            retries = self.retransmit_counts.get(seq, 0) + 1
                            self.retransmit_counts[seq] = retries

                            if retries > MAX_RETRANSMISSIONS:
                                self._log(f"[ERROR] Seq={seq} exceeded max retransmissions ({MAX_RETRANSMISSIONS})")
                                self.stop_event.set()
                                return False

                            self._log(f"[TIMEOUT] Seq={seq} timed out after {self.timeout:.2f}s (retry {retries})")
                            self.timers[seq] = now
                            self._send_packet(self.packets[seq], is_retransmit=True)

            if self.finished.is_set():
                break

            time.sleep(TIMER_CHECK_INTERVAL)

        if self.success:
            self._log(f"[COMPLETE] Chunk {self.chunk_id} transfer completed successfully!")
            # Send FIN packet
            fin_pkt = Packet.create_fin(chunk_id=self.chunk_id, total_packets=self.total_packets)
            try:
                self.sock.sendto(fin_pkt.encode(), self.dest_addr)
            except socket.error:
                pass
            return True
        return False

    def stop(self) -> None:
        """Signal sender loop to stop."""
        self.stop_event.set()
