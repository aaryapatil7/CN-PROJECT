"""
Network Statistics and Transfer Metrics Tracker.

Tracks packets sent, received, intentionally lost, retransmitted, ACKs,
transfer durations, throughput, and packet loss rates.
"""

import time
import threading
from typing import Dict, Any


class NetworkStats:
    """
    Thread-safe tracker for network metrics and protocol performance.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        """Reset all counters and timers."""
        with self.lock:
            self.start_time: float = time.time()
            self.end_time: float = self.start_time
            self.packets_sent: int = 0
            self.packets_received: int = 0
            self.packets_lost: int = 0
            self.packets_retransmitted: int = 0
            self.packets_corrupted: int = 0
            self.packets_buffered: int = 0
            self.duplicate_packets: int = 0
            self.acks_sent: int = 0
            self.acks_received: int = 0
            self.acks_lost: int = 0
            self.bytes_transferred: int = 0

    def record(self, key: str, count: int = 1) -> None:
        """Increment a specific metric counter."""
        with self.lock:
            if hasattr(self, key):
                setattr(self, key, getattr(self, key) + count)

    def mark_start(self) -> None:
        """Mark start timestamp of an active transfer."""
        with self.lock:
            self.start_time = time.time()
            self.end_time = self.start_time

    def mark_end(self) -> None:
        """Mark completion timestamp of an active transfer."""
        with self.lock:
            self.end_time = time.time()

    def get_summary(self) -> Dict[str, Any]:
        """Compute derived transfer metrics."""
        with self.lock:
            elapsed = max(0.001, (self.end_time if self.end_time > self.start_time else time.time()) - self.start_time)
            total_data_packets = self.packets_sent + self.packets_retransmitted
            loss_rate = (self.packets_lost / total_data_packets * 100.0) if total_data_packets > 0 else 0.0
            retransmit_rate = (self.packets_retransmitted / self.packets_sent * 100.0) if self.packets_sent > 0 else 0.0
            throughput_mb_s = (self.bytes_transferred / (1024 * 1024)) / elapsed if elapsed > 0 else 0.0
            throughput_kb_s = (self.bytes_transferred / 1024) / elapsed if elapsed > 0 else 0.0

            return {
                "elapsed_seconds": elapsed,
                "packets_sent": self.packets_sent,
                "packets_received": self.packets_received,
                "packets_lost": self.packets_lost,
                "packets_retransmitted": self.packets_retransmitted,
                "packets_corrupted": self.packets_corrupted,
                "packets_buffered": self.packets_buffered,
                "duplicate_packets": self.duplicate_packets,
                "acks_sent": self.acks_sent,
                "acks_received": self.acks_received,
                "bytes_transferred": self.bytes_transferred,
                "loss_rate_pct": loss_rate,
                "retransmit_rate_pct": retransmit_rate,
                "throughput_mb_s": throughput_mb_s,
                "throughput_kb_s": throughput_kb_s,
            }
