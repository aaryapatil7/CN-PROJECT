"""
Selective Repeat Protocol Session Coordinator.

Manages active sender and receiver sessions for UDP-based chunk transfers.
"""

import time
import socket
import threading
from typing import Dict, Optional, Callable, Tuple

from .packet import Packet, PacketType
from .sender import SRSender
from .receiver import SRReceiver
from config import DEFAULT_WINDOW_SIZE, DEFAULT_TIMEOUT


class SelectiveRepeatSession:
    """
    Coordinates Selective Repeat transmissions and receptions across peers.
    """

    def __init__(
        self,
        sock: socket.socket,
        window_size: int = DEFAULT_WINDOW_SIZE,
        timeout: float = DEFAULT_TIMEOUT,
        loss_prob: float = 0.0,
        corruption_prob: float = 0.0,
        log_callback: Optional[Callable[[str], None]] = None,
        stats_callback: Optional[Callable[[str, int], None]] = None,
    ):
        self.sock = sock
        self.window_size = window_size
        self.timeout = timeout
        self.loss_prob = loss_prob
        self.corruption_prob = corruption_prob
        self.log_callback = log_callback or (lambda msg: None)
        self.stats_callback = stats_callback or (lambda key, val: None)

        # Active senders: (dest_addr, chunk_id) -> SRSender
        self.active_senders: Dict[Tuple[Tuple[str, int], int], SRSender] = {}
        # Active receivers: (src_addr, chunk_id) -> SRReceiver
        self.active_receivers: Dict[Tuple[Tuple[str, int], int], SRReceiver] = {}
        self.lock = threading.Lock()

    def send_chunk_data(
        self,
        dest_addr: Tuple[str, int],
        chunk_id: int,
        data: bytes,
        blocking: bool = True,
    ) -> bool:
        """
        Create and run a Selective Repeat Sender for the given chunk.
        """
        sender = SRSender(
            sock=self.sock,
            dest_addr=dest_addr,
            chunk_id=chunk_id,
            data=data,
            window_size=self.window_size,
            timeout=self.timeout,
            loss_prob=self.loss_prob,
            corruption_prob=self.corruption_prob,
            log_callback=self.log_callback,
            stats_callback=self.stats_callback,
        )

        key = (dest_addr, chunk_id)
        with self.lock:
            self.active_senders[key] = sender

        try:
            if blocking:
                return sender.send_chunk()
            else:
                t = threading.Thread(target=sender.send_chunk, daemon=True)
                t.start()
                return True
        finally:
            if blocking:
                with self.lock:
                    self.active_senders.pop(key, None)

    def receive_chunk_data(
        self,
        sender_addr: Tuple[str, int],
        chunk_id: int,
        timeout_seconds: float = 30.0,
    ) -> Optional[bytes]:
        """
        Wait for and assemble a chunk from a sender using Selective Repeat.
        """
        receiver = SRReceiver(
            sock=self.sock,
            sender_addr=sender_addr,
            chunk_id=chunk_id,
            window_size=self.window_size,
            log_callback=self.log_callback,
            stats_callback=self.stats_callback,
        )

        key = (sender_addr, chunk_id)
        with self.lock:
            self.active_receivers[key] = receiver

        try:
            # Wait until all packets have arrived or timeout occurs
            is_done = receiver.finished.wait(timeout=timeout_seconds)
            if is_done:
                return receiver.get_assembled_data()
            self.log_callback(f"[ERROR] Timeout waiting for Chunk {chunk_id} from {sender_addr}")
            return None
        finally:
            with self.lock:
                self.active_receivers.pop(key, None)

    def route_incoming_packet(self, pkt: Packet, src_addr: Tuple[str, int]) -> None:
        """
        Route an incoming packet to its corresponding active sender or receiver.
        """
        if pkt.pkt_type == PacketType.ACK:
            # Route ACK to active sender
            key = (src_addr, pkt.chunk_id)
            with self.lock:
                sender = self.active_senders.get(key)
            if sender:
                sender.handle_ack(pkt)

        elif pkt.pkt_type in (PacketType.DATA, PacketType.FIN):
            # Route DATA or FIN to active receiver
            key = (src_addr, pkt.chunk_id)
            with self.lock:
                receiver = self.active_receivers.get(key)
            if receiver:
                receiver.handle_packet(pkt)
