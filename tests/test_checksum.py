"""
Unit tests for CRC32 checksum calculation and verification.
"""

import unittest
from protocol.checksum import calculate_checksum, verify_checksum, corrupt_bytes


class TestChecksum(unittest.TestCase):
    def test_checksum_calculation_and_verification(self):
        data = b"Hello, Selective Repeat Protocol over UDP!"
        crc = calculate_checksum(data)
        self.assertIsInstance(crc, int)
        self.assertTrue(verify_checksum(data, crc))

    def test_corrupted_data_detection(self):
        data = b"Critical network payload data"
        crc = calculate_checksum(data)
        corrupted = corrupt_bytes(data, num_bit_flips=1)
        self.assertFalse(verify_checksum(corrupted, crc))

    def test_empty_data(self):
        data = b""
        crc = calculate_checksum(data)
        self.assertTrue(verify_checksum(data, crc))


if __name__ == "__main__":
    unittest.main()
