"""
SHA-256 Hashing and Integrity Verification.

Provides streaming SHA-256 calculation for large files and byte arrays
to ensure cryptographic end-to-end data integrity.
"""

import os
import hashlib


def calculate_file_sha256(file_path: str, block_size: int = 65536) -> str:
    """
    Calculate the SHA-256 hash of a file using buffered streaming reads.

    Args:
        file_path: Path to the target file.
        block_size: Buffer size for streaming read operations (default 64 KB).

    Returns:
        Hexadecimal SHA-256 digest string.
    """
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(block_size):
            hasher.update(chunk)
    return hasher.hexdigest()


def calculate_bytes_sha256(data: bytes) -> str:
    """
    Calculate the SHA-256 hash of an in-memory byte buffer.

    Args:
        data: Byte buffer.

    Returns:
        Hexadecimal SHA-256 digest string.
    """
    return hashlib.sha256(data).hexdigest()


def verify_file_sha256(file_path: str, expected_sha256: str) -> bool:
    """
    Verify whether the target file's SHA-256 matches the expected digest.

    Args:
        file_path: Path to the file.
        expected_sha256: Expected hexadecimal SHA-256 digest.

    Returns:
        True if digests match exactly, False otherwise.
    """
    if not os.path.isfile(file_path):
        return False
    actual_hash = calculate_file_sha256(file_path)
    return actual_hash.lower() == expected_sha256.lower()
