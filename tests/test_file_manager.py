"""
Unit tests for file manager (chunking, reassembly, resume, SHA-256 integrity).
"""

import os
import shutil
import tempfile
import unittest

from file_manager.chunk_manager import ChunkManager
from file_manager.file_reassembler import FileReassembler
from file_manager.integrity import calculate_file_sha256, verify_file_sha256


class TestFileManager(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.shared_dir = os.path.join(self.test_dir, "shared")
        self.downloads_dir = os.path.join(self.test_dir, "downloads")
        os.makedirs(self.shared_dir, exist_ok=True)
        os.makedirs(self.downloads_dir, exist_ok=True)

        # Create a sample test file (150 KB -> ~3 chunks with 64 KB chunk size)
        self.sample_filename = "test_document.bin"
        self.sample_path = os.path.join(self.shared_dir, self.sample_filename)
        self.file_data = os.urandom(150 * 1024)
        with open(self.sample_path, "wb") as f:
            f.write(self.file_data)

        self.expected_sha256 = calculate_file_sha256(self.sample_path)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_chunking_and_metadata(self):
        chunk_mgr = ChunkManager(self.shared_dir, chunk_size=64 * 1024)
        meta = chunk_mgr.get_file_metadata(self.sample_filename)

        self.assertIsNotNone(meta)
        self.assertEqual(meta["file_name"], self.sample_filename)
        self.assertEqual(meta["file_size"], len(self.file_data))
        self.assertEqual(meta["total_chunks"], 3)
        self.assertEqual(meta["file_sha256"], self.expected_sha256)

        # Verify reading individual chunks
        c0 = chunk_mgr.read_chunk(self.sample_filename, 0)
        c1 = chunk_mgr.read_chunk(self.sample_filename, 1)
        c2 = chunk_mgr.read_chunk(self.sample_filename, 2)

        self.assertEqual(len(c0), 64 * 1024)
        self.assertEqual(len(c1), 64 * 1024)
        self.assertEqual(len(c2), (150 - 128) * 1024)
        self.assertEqual(c0 + c1 + c2, self.file_data)

    def test_reassembly_and_integrity_verification(self):
        chunk_mgr = ChunkManager(self.shared_dir, chunk_size=64 * 1024)
        reassembler = FileReassembler(self.downloads_dir)

        # Simulate receiving chunks 0, 1, 2
        for cid in range(3):
            data = chunk_mgr.read_chunk(self.sample_filename, cid)
            reassembler.save_chunk(self.sample_filename, cid, data)

        success, msg, final_path = reassembler.reassemble_file(
            self.sample_filename, total_chunks=3, expected_sha256=self.expected_sha256
        )

        self.assertTrue(success)
        self.assertTrue(os.path.isfile(final_path))
        self.assertTrue(verify_file_sha256(final_path, self.expected_sha256))

        with open(final_path, "rb") as f:
            self.assertEqual(f.read(), self.file_data)

    def test_resume_missing_chunks_detection(self):
        chunk_mgr = ChunkManager(self.shared_dir, chunk_size=64 * 1024)
        reassembler = FileReassembler(self.downloads_dir)

        # Save only chunk 0 and chunk 2 (simulate interrupted transfer where chunk 1 is missing)
        c0 = chunk_mgr.read_chunk(self.sample_filename, 0)
        c2 = chunk_mgr.read_chunk(self.sample_filename, 2)
        reassembler.save_chunk(self.sample_filename, 0, c0)
        reassembler.save_chunk(self.sample_filename, 2, c2)

        missing = reassembler.get_missing_chunks(self.sample_filename, total_chunks=3)
        self.assertEqual(missing, [1])

        # Attempting reassembly with missing chunk should fail gracefully
        success, msg, _ = reassembler.reassemble_file(
            self.sample_filename, total_chunks=3, expected_sha256=self.expected_sha256
        )
        self.assertFalse(success)
        self.assertIn("Missing chunks", msg)

        # Now supply missing chunk 1 and reassemble
        c1 = chunk_mgr.read_chunk(self.sample_filename, 1)
        reassembler.save_chunk(self.sample_filename, 1, c1)

        missing_now = reassembler.get_missing_chunks(self.sample_filename, total_chunks=3)
        self.assertEqual(missing_now, [])

        success, msg, final_path = reassembler.reassemble_file(
            self.sample_filename, total_chunks=3, expected_sha256=self.expected_sha256
        )
        self.assertTrue(success)


if __name__ == "__main__":
    unittest.main()
