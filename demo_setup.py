"""
Classroom Demonstration Setup Script.

Prepares sample test files (movie.mp4, notes.pdf, image.jpg) and sets up
distinct shared and download directories for 3 local peers (Peer A, Peer B, Peer C).
"""

import os
import sys
import hashlib
from config import BASE_DIR

PEER_DIRS = {
    "PEER_A": {"port": 5001, "dir": os.path.join(BASE_DIR, "shared_files", "peer_a"), "down": os.path.join(BASE_DIR, "downloads", "peer_a")},
    "PEER_B": {"port": 5002, "dir": os.path.join(BASE_DIR, "shared_files", "peer_b"), "down": os.path.join(BASE_DIR, "downloads", "peer_b")},
    "PEER_C": {"port": 5003, "dir": os.path.join(BASE_DIR, "shared_files", "peer_c"), "down": os.path.join(BASE_DIR, "downloads", "peer_c")},
}


def create_dummy_file(filepath: str, size_bytes: int, pattern: bytes) -> str:
    """Create a structured dummy file with a deterministic pattern and return SHA-256."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    hasher = hashlib.sha256()
    written = 0
    with open(filepath, "wb") as f:
        while written < size_bytes:
            to_write = min(len(pattern), size_bytes - written)
            f.write(pattern[:to_write])
            hasher.update(pattern[:to_write])
            written += to_write
    return hasher.hexdigest()


def setup_demo_environment():
    """Create demo files and directories for 3-peer demonstration."""
    print("\n===============================================================")
    print("      PREPARING 3-PEER CLASSROOM DEMONSTRATION ENVIRONMENT     ")
    print("===============================================================")

    # Ensure clean directory structure
    for p_id, p_info in PEER_DIRS.items():
        os.makedirs(p_info["dir"], exist_ok=True)
        os.makedirs(p_info["down"], exist_ok=True)

    # 1. movie.mp4 (1.5 MB) -> Shared by Peer A and Peer C (for multi-peer concurrent download)
    movie_pattern = b"[MP4_FRAME_HEADER]" + (b"\x11\x22\x33\x44\x55\x66\x77\x88" * 512)
    movie_size = 1536 * 1024  # 1.5 MB (~24 chunks of 64KB)
    path_a_movie = os.path.join(PEER_DIRS["PEER_A"]["dir"], "movie.mp4")
    path_c_movie = os.path.join(PEER_DIRS["PEER_C"]["dir"], "movie.mp4")

    sha_movie = create_dummy_file(path_a_movie, movie_size, movie_pattern)
    create_dummy_file(path_c_movie, movie_size, movie_pattern)
    print(f"  [+] Created 'movie.mp4' (1.5 MB, ~24 chunks) in Peer A & Peer C")
    print(f"      SHA-256: {sha_movie}")

    # 2. notes.pdf (512 KB) -> Shared by Peer A
    pdf_pattern = b"%PDF-1.7 Academic Computer Networks Project Notes..." + (b"\xAA\xBB\xCC\xDD" * 256)
    pdf_size = 512 * 1024  # 512 KB (~8 chunks)
    path_a_pdf = os.path.join(PEER_DIRS["PEER_A"]["dir"], "notes.pdf")
    sha_pdf = create_dummy_file(path_a_pdf, pdf_size, pdf_pattern)
    print(f"  [+] Created 'notes.pdf' (512 KB, ~8 chunks) in Peer A")
    print(f"      SHA-256: {sha_pdf}")

    # 3. image.jpg (768 KB) -> Shared by Peer B
    img_pattern = b"\xFF\xD8\xFF\xE0JFIF Header..." + (b"\xDE\xAD\xBE\xEF" * 256)
    img_size = 768 * 1024  # 768 KB (~12 chunks)
    path_b_img = os.path.join(PEER_DIRS["PEER_B"]["dir"], "image.jpg")
    sha_img = create_dummy_file(path_b_img, img_size, img_pattern)
    print(f"  [+] Created 'image.jpg' (768 KB, ~12 chunks) in Peer B")
    print(f"      SHA-256: {sha_img}")

    print("\n---------------------------------------------------------------")
    print("DEMONSTRATION ON 3 SEPARATE COMPUTERS (SAME LAN):")
    print("---------------------------------------------------------------")
    print("Computer 1 (10.30.164.22) - Peer A (Seeder):")
    print("  python main.py --peer-id PEER_A --port 5001\n")
    print("Computer 3 (10.30.164.24) - Peer C (Seeder):")
    print("  python main.py --peer-id PEER_C --port 5001\n")
    print("Computer 2 (10.30.164.23) - Peer B (Downloader):")
    print("  python main.py --peer-id PEER_B --port 5001 --loss 0.10\n")
    print("  * On Peer B, select option [1] to view connected peers.")
    print("  * Select option [2] to download 'movie.mp4'.")
    print("  * Chunks will download concurrently from 10.30.164.22 and 10.30.164.24!\n")
    print("---------------------------------------------------------------")
    print("SINGLE-COMPUTER LOCAL DEMO (3 Terminals on same machine):")
    print("---------------------------------------------------------------")
    print("Terminal 1 (Peer A): python main.py --peer-id PEER_A --port 5001")
    print("Terminal 2 (Peer C): python main.py --peer-id PEER_C --port 5003")
    print("Terminal 3 (Peer B): python main.py --peer-id PEER_B --port 5002 --loss 0.10")
    print("===============================================================\n")


if __name__ == "__main__":
    setup_demo_environment()
