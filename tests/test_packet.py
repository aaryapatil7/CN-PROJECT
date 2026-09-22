"""
Unit tests for custom Application-Layer Packet serialization and deserialization.
"""

import unittest
from protocol.packet import Packet, PacketType


class TestPacket(unittest.TestCase):
    def test_data_packet_encode_decode(self):
        payload = b"Sample chunk data block for transmission"
        pkt = Packet.create_data(seq_num=42, chunk_id=3, payload=payload)
        raw = pkt.encode()

        decoded = Packet.decode(raw)
        self.assertEqual(decoded.pkt_type, PacketType.DATA)
        self.assertEqual(decoded.seq_num, 42)
        self.assertEqual(decoded.chunk_id, 3)
        self.assertEqual(decoded.payload, payload)
        self.assertEqual(decoded.payload_len, len(payload))

    def test_ack_packet(self):
        ack = Packet.create_ack(seq_num=15, chunk_id=2)
        raw = ack.encode()
        decoded = Packet.decode(raw)

        self.assertEqual(decoded.pkt_type, PacketType.ACK)
        self.assertEqual(decoded.seq_num, 15)
        self.assertEqual(decoded.chunk_id, 2)
        self.assertEqual(decoded.payload_len, 0)

    def test_json_control_packets(self):
        req = Packet.create_request("video.mp4", chunk_id=5)
        raw = req.encode()
        decoded = Packet.decode(raw)
        data = decoded.payload_as_json()
        self.assertEqual(data["file_name"], "video.mp4")
        self.assertEqual(data["chunk_id"], 5)

        disc = Packet.create_discovery("PEER_A", 5001, ["notes.pdf", "img.png"])
        d_raw = disc.encode()
        d_decoded = Packet.decode(d_raw)
        d_data = d_decoded.payload_as_json()
        self.assertEqual(d_data["peer_id"], "PEER_A")
        self.assertEqual(d_data["port"], 5001)
        self.assertEqual(d_data["shared_files"], ["notes.pdf", "img.png"])

    def test_corrupted_packet_rejected(self):
        pkt = Packet.create_data(seq_num=7, chunk_id=1, payload=b"Sensitive Data Payload")
        raw = bytearray(pkt.encode())
        # Corrupt one byte in payload
        raw[-1] ^= 0xFF
        with self.assertRaises(ValueError):
            Packet.decode(bytes(raw))

    def test_invalid_magic_cookie_rejected(self):
        pkt = Packet.create_ack(seq_num=1, chunk_id=0)
        raw = bytearray(pkt.encode())
        raw[0] = 0x00  # corrupt magic cookie
        with self.assertRaises(ValueError):
            Packet.decode(bytes(raw))


if __name__ == "__main__":
    unittest.main()
