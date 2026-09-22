"""
Selective Repeat Sliding Window Receiver implementation over UDP.

Provides reliable data reception using an out-of-order packet buffer,
individual selective ACKs, duplicate packet filtering, and sequential delivery.
"""

import socket
import random
import threading
from typing import Dict, Optional, Callable

from .packet import Packet, PacketType
from config import DEFAULT_WINDOW_SIZE


class SRReceiver:
    """
    Selective Repeat Receiver.

    Receives packets for a single chunk, sends selective ACKs for every valid packet,
    buffers out-of-order arrivals within the receive window, and delivers packets
    in strict sequence number order.
    """

    def __init__(
        self,
        sock: socket.socket,
        sender_addr: tuple[str, int],
        chunk_id: int,
        window_size: int = DEFAULT_WINDOW_SIZE,
        ack_loss_prob: float = 0.0,
        log_callback: Optional[Callable[[str], None]] = None,
        stats_callback: Optional[Callable[[str, int], None]] = None,
    ):
        self.sock = sock
        self.sender_addr = sender_addr
        self.chunk_id = chunk_id
        self.window_size = window_size
        self.ack_loss_prob = ack_loss_prob
        self.log_callback = log_callback or (lambda msg: None)
        self.stats_callback = stats_callback or (lambda key, val: None)

        # Sliding receive window: [rcv_base, rcv_base + window_size - 1]
        self.rcv_base = 0
        self.buffer: Dict[int, bytes] = {}          # Out-of-order packets: seq -> payload
        self.received_slices: Dict[int, bytes] = {}  # In-order delivered packets: seq -> payload
        self.total_packets: Optional[int] = None
        self.is_fin_seen = False

        self.lock = threading.Lock()
        self.finished = threading.Event()

    def _log(self, message: str) -> None:
        self.log_callback(message)

    def _record_stat(self, key: str, val: int = 1) -> None:
        self.stats_callback(key, val)

    def _send_ack(self, seq_num: int) -> None:
        """Send selective ACK packet for a received sequence number."""
        # Simulated ACK loss
        if self.ack_loss_prob > 0.0 and random.random() < self.ack_loss_prob:
            self._record_stat("acks_lost", 1)
            self._log(f"[LOSS] Simulated loss of ACK={seq_num} for Chunk {self.chunk_id}")
            return

        ack_pkt = Packet.create_ack(seq_num=seq_num, chunk_id=self.chunk_id)
        try:
            self.sock.sendto(ack_pkt.encode(), self.sender_addr)
            self._record_stat("acks_sent", 1)
            self._log(f"[ACK] Sent ACK={seq_num} for Chunk {self.chunk_id}")
        except socket.error as e:
            self._log(f"[ERROR] Failed to send ACK={seq_num}: {e}")

    def handle_packet(self, pkt: Packet) -> None:
        """
        Process an incoming DATA or FIN packet for this chunk.
        """
        if pkt.chunk_id != self.chunk_id:
            return

        with self.lock:
            if pkt.pkt_type == PacketType.FIN:
                self._record_stat("packets_received", 1)
                try:
                    payload_info = pkt.payload_as_json()
                    self.total_packets = payload_info.get("total_packets", self.rcv_base)
                except Exception:
                    self.total_packets = self.rcv_base

                self.is_fin_seen = True
                self._check_completion()
                return

            if pkt.pkt_type != PacketType.DATA:
                return

            self._record_stat("packets_received", 1)
            seq = pkt.seq_num
            self._log(
                f"[DATA] Received Seq={seq} Chunk={pkt.chunk_id} ({pkt.payload_len} bytes) "
                f"[Window: {self.rcv_base}..{self.rcv_base + self.window_size - 1}]"
            )

            # Check if this packet is marked with FIN flag
            if pkt.flags & Packet.FLAG_FIN:
                self.total_packets = seq + 1
                self.is_fin_seen = True

            # Case 1: Packet falls inside receive window [rcv_base, rcv_base + window_size - 1]
            if self.rcv_base <= seq < self.rcv_base + self.window_size:
                # Always send ACK for packets in receive window
                self._send_ack(seq)

                if seq == self.rcv_base:
                    # In-order packet arrived! Deliver it
                    self.received_slices[seq] = pkt.payload
                    old_base = self.rcv_base
                    self.rcv_base += 1

                    # Deliver any consecutive packets previously buffered
                    delivered_buffered = []
                    while self.rcv_base in self.buffer:
                        self.received_slices[self.rcv_base] = self.buffer.pop(self.rcv_base)
                        delivered_buffered.append(self.rcv_base)
                        self.rcv_base += 1

                    if delivered_buffered:
                        self._log(
                            f"[BUFFER] Delivered buffered packets: {delivered_buffered}, "
                            f"rcv_base advanced from {old_base} to {self.rcv_base}"
                        )
                    else:
                        self._log(f"[WINDOW] Rcv window advanced: rcv_base={self.rcv_base}")

                    self._check_completion()
                else:
                    # Out-of-order packet within window: Buffer it!
                    if seq not in self.buffer:
                        self.buffer[seq] = pkt.payload
                        self._record_stat("packets_buffered", 1)
                        self._log(
                            f"[BUFFER] Stored out-of-order Seq={seq} (waiting for Seq={self.rcv_base})"
                        )

            # Case 2: Packet was already acknowledged in previous window [rcv_base - window_size, rcv_base - 1]
            elif max(0, self.rcv_base - self.window_size) <= seq < self.rcv_base:
                self._record_stat("duplicate_packets", 1)
                self._log(f"[DUP] Received duplicate Seq={seq}, re-sending ACK={seq}")
                self._send_ack(seq)

            # Case 3: Packet outside window (ignore/drop)
            else:
                self._log(f"[DROP] Packet Seq={seq} outside receive window [{self.rcv_base}..{self.rcv_base + self.window_size - 1}]")

    def _check_completion(self) -> None:
        """Check if all packets have been received in-order and transfer is complete."""
        if self.is_fin_seen and self.total_packets is not None:
            if self.rcv_base >= self.total_packets and len(self.received_slices) >= self.total_packets:
                self.finished.set()
                self._log(f"[COMPLETE] Receiver successfully assembled all {self.total_packets} packets for Chunk {self.chunk_id}!")

    def get_assembled_data(self) -> bytes:
        """Assemble all in-order packets into a contiguous byte chunk."""
        with self.lock:
            if self.total_packets is None:
                total = self.rcv_base
            else:
                total = self.total_packets
            return b"".join(self.received_slices.get(i, b"") for i in range(total))
