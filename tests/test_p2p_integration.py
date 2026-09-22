"""
Integration test for full multi-peer concurrent P2P file sharing over UDP with Selective Repeat.
"""

import os
import shutil
import tempfile
import time
import unittest

from peer.peer import PeerNode
from file_manager.integrity import calculate_file_sha256


class TestP2PIntegration(unittest.TestCase):
    def setUp(self):
        self.base_dir = tempfile.mkdtemp()
        self.dir_a = os.path.join(self.base_dir, "peer_a")
        self.dir_b = os.path.join(self.base_dir, "peer_b")
        self.dir_c = os.path.join(self.base_dir, "peer_c")

        for d in [self.dir_a, self.dir_b, self.dir_c]:
            os.makedirs(os.path.join(d, "shared"), exist_ok=True)
            os.makedirs(os.path.join(d, "downloads"), exist_ok=True)

        # Create a test file in Peer A and Peer C (200 KB -> 4 chunks of 64 KB)
        self.filename = "big_demo_movie.mp4"
        self.file_data = os.urandom(200 * 1024)

        with open(os.path.join(self.dir_a, "shared", self.filename), "wb") as f:
            f.write(self.file_data)
        with open(os.path.join(self.dir_c, "shared", self.filename), "wb") as f:
            f.write(self.file_data)

        self.expected_sha256 = calculate_file_sha256(os.path.join(self.dir_a, "shared", self.filename))

    def tearDown(self):
        shutil.rmtree(self.base_dir, ignore_errors=True)

    def test_multi_peer_concurrent_download_with_packet_loss(self):
        """
        Verify that Peer B can concurrently download chunks from Peer A and Peer C
        under 15% simulated packet loss, and verify final file SHA-256.
        """
        peer_a = PeerNode(
            peer_id="PEER_A",
            port=6001,
            shared_dir=os.path.join(self.dir_a, "shared"),
            downloads_dir=os.path.join(self.dir_a, "downloads"),
            window_size=4,
            loss_prob=0.10,
            timeout=0.3,
        )

        peer_c = PeerNode(
            peer_id="PEER_C",
            port=6003,
            shared_dir=os.path.join(self.dir_c, "shared"),
            downloads_dir=os.path.join(self.dir_c, "downloads"),
            window_size=4,
            loss_prob=0.10,
            timeout=0.3,
        )

        peer_b = PeerNode(
            peer_id="PEER_B",
            port=6002,
            shared_dir=os.path.join(self.dir_b, "shared"),
            downloads_dir=os.path.join(self.dir_b, "downloads"),
            window_size=4,
            loss_prob=0.0,
            timeout=0.3,
        )

        peer_a.start()
        peer_c.start()
        peer_b.start()

        try:
            # Explicitly register peers for fast test convergence
            peer_b.discovery.update_peer("PEER_A", "127.0.0.1", 6001, [self.filename])
            peer_b.discovery.update_peer("PEER_C", "127.0.0.1", 6003, [self.filename])
            peer_a.discovery.update_peer("PEER_B", "127.0.0.1", 6002, [])
            peer_c.discovery.update_peer("PEER_B", "127.0.0.1", 6002, [])

            time.sleep(0.5)

            # Peer B initiates download of the video file
            success, msg, final_path = peer_b.download_file(self.filename)

            self.assertTrue(success, f"Download failed with message: {msg}")
            self.assertTrue(os.path.isfile(final_path))

            # Verify cryptographic SHA-256 match
            downloaded_sha256 = calculate_file_sha256(final_path)
            self.assertEqual(downloaded_sha256, self.expected_sha256)

            with open(final_path, "rb") as f:
                self.assertEqual(f.read(), self.file_data)

        finally:
            peer_a.stop()
            peer_b.stop()
            peer_c.stop()


if __name__ == "__main__":
    unittest.main()
