"""
Checksum and Data Integrity utilities for packet error detection.

Uses standard 32-bit Cyclic Redundancy Check (CRC-32) algorithm to detect
bit-level transmission corruption in headers and payload.
"""

import zlib
import random


def calculate_checksum(data: bytes) -> int:
    """
    Calculate 32-bit CRC32 checksum over the provided byte sequence.

    Args:
        data: Byte string to compute checksum over.

    Returns:
        32-bit unsigned integer checksum (0 to 2^32 - 1).
    """
    return zlib.crc32(data) & 0xFFFFFFFF


def verify_checksum(data: bytes, expected_checksum: int) -> bool:
    """
    Verify whether the computed checksum of the data matches the expected checksum.

    Args:
        data: Byte string to verify.
        expected_checksum: 32-bit checksum value extracted from packet header.

    Returns:
        True if valid and uncorrupted, False if corrupted.
    """
    computed = calculate_checksum(data)
    return computed == (expected_checksum & 0xFFFFFFFF)


def corrupt_bytes(data: bytes, num_bit_flips: int = 1) -> bytes:
    """
    Simulate transmission corruption by flipping bits in the byte array.

    Args:
        data: Original byte sequence.
        num_bit_flips: Number of random byte positions to alter.

    Returns:
        Corrupted byte sequence.
    """
    if not data:
        return data
    ba = bytearray(data)
    for _ in range(num_bit_flips):
        idx = random.randint(0, len(ba) - 1)
        ba[idx] ^= (1 << random.randint(0, 7))
    return bytes(ba)
