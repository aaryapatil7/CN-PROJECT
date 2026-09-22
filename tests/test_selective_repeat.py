"""
Unit tests for Selective Repeat sliding window protocol over UDP.

Verifies:
- Lossless data transmission and in-order assembly
- Selective retransmission under simulated packet loss
- Out-of-order packet buffering and sequential delivery
- Duplicate packet filtering
"""

import time
import socket
import threading
import unittest

from protocol.packet import Packet, PacketType
from protocol.sender import SRSender
from protocol.receiver import SRReceiver


class TestSelectiveRepeat(unittest.TestCase):
    def setUp(self):
        # Create loopback UDP sockets for sender and receiver
        self.sock_sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_sender.bind(("127.0.0.1", 0))
        self.sender_addr = self.sock_sender.getsockname()

        self.sock_receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_receiver.bind(("127.0.0.1", 0))
        self.receiver_addr = self.sock_receiver.getsockname()

        self.stop_event = threading.Event()

    def tearDown(self):
        self.stop_event.set()
        self.sock_sender.close()
        self.sock_receiver.close()

    def test_lossless_transmission(self):
        """Test transmission without packet loss."""
        # 10 KB test payload (~8 packets of 1400 bytes)
        payload = b"A" * (10 * 1024)
        chunk_id = 0

        receiver = SRReceiver(
            sock=self.sock_receiver,
            sender_addr=self.sender_addr,
            chunk_id=chunk_id,
            window_size=4,
        )

        sender = SRSender(
            sock=self.sock_sender,
            dest_addr=self.receiver_addr,
            chunk_id=chunk_id,
            data=payload,
            window_size=4,
            timeout=0.3,
            loss_prob=0.0,
        )

        # Receiver socket loop
        def receiver_loop():
            self.sock_receiver.settimeout(0.1)
            while not receiver.finished.is_set() and not self.stop_event.is_set():
                try:
                    data, addr = self.sock_receiver.recvfrom(4096)
                    pkt = Packet.decode(data)
                    receiver.handle_packet(pkt)
                except (socket.timeout, ValueError):
                    continue

        # Sender ACK loop
        def sender_ack_loop():
            self.sock_sender.settimeout(0.1)
            while not sender.finished.is_set() and not self.stop_event.is_set():
                try:
                    data, addr = self.sock_sender.recvfrom(4096)
                    pkt = Packet.decode(data)
                    if pkt.pkt_type == PacketType.ACK:
                        sender.handle_ack(pkt)
                except (socket.timeout, ValueError):
                    continue

        t_rcv = threading.Thread(target=receiver_loop, daemon=True)
        t_ack = threading.Thread(target=sender_ack_loop, daemon=True)
        t_rcv.start()
        t_ack.start()

        success = sender.send_chunk(blocking=True)
        self.assertTrue(success)

        # Wait for receiver to finish
        self.assertTrue(receiver.finished.wait(timeout=2.0))
        assembled = receiver.get_assembled_data()
        self.assertEqual(len(assembled), len(payload))
        self.assertEqual(assembled, payload)

    def test_selective_retransmission_with_packet_loss(self):
        """Test transmission under 25% simulated packet loss."""
        payload = b"B" * (20 * 1024)  # ~15 packets
        chunk_id = 1
        retransmissions = []

        def stats_cb(key, val):
            if key == "packets_retransmitted":
                retransmissions.append(1)

        receiver = SRReceiver(
            sock=self.sock_receiver,
            sender_addr=self.sender_addr,
            chunk_id=chunk_id,
            window_size=4,
        )

        sender = SRSender(
            sock=self.sock_sender,
            dest_addr=self.receiver_addr,
            chunk_id=chunk_id,
            data=payload,
            window_size=4,
            timeout=0.2,
            loss_prob=0.25,
            stats_callback=stats_cb,
        )

        def receiver_loop():
            self.sock_receiver.settimeout(0.1)
            while not receiver.finished.is_set() and not self.stop_event.is_set():
                try:
                    data, addr = self.sock_receiver.recvfrom(4096)
                    pkt = Packet.decode(data)
                    receiver.handle_packet(pkt)
                except (socket.timeout, ValueError):
                    continue

        def sender_ack_loop():
            self.sock_sender.settimeout(0.1)
            while not sender.finished.is_set() and not self.stop_event.is_set():
                try:
                    data, addr = self.sock_sender.recvfrom(4096)
                    pkt = Packet.decode(data)
                    if pkt.pkt_type == PacketType.ACK:
                        sender.handle_ack(pkt)
                except (socket.timeout, ValueError):
                    continue

        t_rcv = threading.Thread(target=receiver_loop, daemon=True)
        t_ack = threading.Thread(target=sender_ack_loop, daemon=True)
        t_rcv.start()
        t_ack.start()

        success = sender.send_chunk(blocking=True)
        self.assertTrue(success)
        self.assertTrue(receiver.finished.wait(timeout=5.0))

        assembled = receiver.get_assembled_data()
        self.assertEqual(assembled, payload)

    def test_out_of_order_buffering(self):
        """Directly verify receiver buffers out-of-order packets and delivers sequentially."""
        chunk_id = 2
        receiver = SRReceiver(
            sock=self.sock_receiver,
            sender_addr=self.sender_addr,
            chunk_id=chunk_id,
            window_size=4,
        )

        # Create 4 packets: 0, 1, 2, 3
        p0 = Packet.create_data(seq_num=0, chunk_id=chunk_id, payload=b"Part 0;")
        p1 = Packet.create_data(seq_num=1, chunk_id=chunk_id, payload=b"Part 1;")
        p2 = Packet.create_data(seq_num=2, chunk_id=chunk_id, payload=b"Part 2;")
        p3 = Packet.create_data(seq_num=3, chunk_id=chunk_id, payload=b"Part 3;", is_fin=True)

        # Deliver out of order: 0, then 2, 3 (skip 1)
        receiver.handle_packet(p0)
        self.assertEqual(receiver.rcv_base, 1)

        receiver.handle_packet(p2)  # Should be buffered!
        self.assertEqual(receiver.rcv_base, 1)
        self.assertIn(2, receiver.buffer)

        receiver.handle_packet(p3)  # Should also be buffered!
        self.assertEqual(receiver.rcv_base, 1)
        self.assertIn(3, receiver.buffer)

        # Now deliver missing packet 1 -> Should cause receiver to deliver 1, 2, and 3!
        receiver.handle_packet(p1)
        self.assertEqual(receiver.rcv_base, 4)
        self.assertEqual(len(receiver.buffer), 0)
        self.assertTrue(receiver.finished.is_set())

        assembled = receiver.get_assembled_data()
        self.assertEqual(assembled, b"Part 0;Part 1;Part 2;Part 3;")


if __name__ == "__main__":
    unittest.main()
