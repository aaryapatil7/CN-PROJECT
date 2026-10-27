# =============================================================================
# file_utils.py – File Utility Functions
# =============================================================================
# This file contains helper functions used by both TCP and UDP transfer modes.
#
# Functions:
#   split_file_into_packets(filepath, packet_size)
#       → Reads a file and splits it into a list of byte chunks.
#         Each chunk becomes one UDP packet (or one TCP send call).
#
#   reassemble_file(packets_dict, output_path)
#       → Takes a dictionary {seq_no: data} and writes the chunks
#         to disk in order, reconstructing the original file.
#
#   sha256_hash(filepath)
#       → Returns the SHA-256 hash of a file as a hex string.
#         Used to verify that the received file is identical to the original.
#
#   print_hash_comparison(original_path, received_path)
#       → Prints both hashes side-by-side and shows MATCH ✓ or MISMATCH ✗.
#
#   print_progress(transferred, total, label)
#       → Prints a simple text progress bar to the terminal.
#
#   list_shared_files(shared_folder)
#       → Lists all files in the shared folder.
# =============================================================================

import os        # For file path and size operations
import hashlib   # For SHA-256 hashing (Python standard library)


def split_file_into_packets(filepath, packet_size):
    """
    Reads a file from disk and splits it into a list of byte chunks.

    Each chunk is at most `packet_size` bytes long.
    The last chunk may be smaller if the file size is not a multiple of packet_size.

    Parameters:
        filepath    (str)  : Full path to the file to split.
        packet_size (int)  : Maximum number of bytes per chunk.

    Returns:
        packets (list of bytes): A list where each element is one chunk of the file.
                                 Index 0 is the first chunk, index 1 is the second, etc.
    """
    packets = []  # This list will hold all the chunks

    # Open the file in binary mode ("rb") so we can read raw bytes.
    # Text mode would corrupt binary files like images or PDFs.
    with open(filepath, "rb") as f:
        while True:
            # Read exactly packet_size bytes from the file.
            # If fewer than packet_size bytes remain, it reads whatever is left.
            chunk = f.read(packet_size)

            if not chunk:
                # f.read() returns an empty bytes object b"" when the file ends.
                break

            packets.append(chunk)  # Add this chunk to our list

    return packets


def reassemble_file(packets_dict, output_path):
    """
    Reconstructs a file from a dictionary of numbered chunks.

    The packets_dict maps sequence numbers to byte data:
        { 0: b"...first chunk...",
          1: b"...second chunk...",
          2: b"...third chunk...", ... }

    The function sorts the keys (sequence numbers) and writes the chunks
    in order to the output file.

    Parameters:
        packets_dict (dict) : {seq_no (int): data (bytes)} – received chunks.
        output_path  (str)  : Path where the reconstructed file should be saved.

    Returns:
        True if successful, False if any sequence numbers are missing.
    """
    if not packets_dict:
        print("[ERROR] No packets to reassemble.")
        return False

    # Sort the sequence numbers so we write chunks in the correct order.
    sorted_seq_numbers = sorted(packets_dict.keys())

    # Quick check: make sure we have a complete, gap-free sequence.
    expected_count = sorted_seq_numbers[-1] + 1  # e.g. if last seq is 49, we expect 50 packets
    if len(sorted_seq_numbers) != expected_count:
        print(f"[ERROR] Missing packets! Expected {expected_count}, got {len(sorted_seq_numbers)}.")
        return False

    # Make sure the output directory exists.
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Write all chunks to the output file in order.
    with open(output_path, "wb") as f:
        for seq_no in sorted_seq_numbers:
            f.write(packets_dict[seq_no])

    print(f"\n[INFO] File reassembled and saved to: {output_path}")
    return True


def sha256_hash(filepath):
    """
    Computes the SHA-256 hash of a file.

    SHA-256 produces a 64-character hexadecimal string that uniquely
    identifies the file contents. Even a single changed byte produces
    a completely different hash — this makes it perfect for verifying
    file integrity after transfer.

    Parameters:
        filepath (str): Path to the file to hash.

    Returns:
        hex_digest (str): 64-character SHA-256 hash string, or None on error.
    """
    # hashlib.sha256() creates a new SHA-256 hash object.
    hasher = hashlib.sha256()

    try:
        # Read the file in 8 KB blocks so large files don't run out of memory.
        with open(filepath, "rb") as f:
            while True:
                block = f.read(8192)  # 8 KB at a time
                if not block:
                    break
                # Feed each block into the hash object.
                # The hash object internally accumulates all the data.
                hasher.update(block)

        # hexdigest() returns the final hash as a hex string.
        return hasher.hexdigest()

    except FileNotFoundError:
        print(f"[ERROR] File not found for hashing: {filepath}")
        return None


def print_hash_comparison(original_path, received_path):
    """
    Computes and prints the SHA-256 hashes of two files side by side.
    Shows MATCH ✓ if they are identical, or MISMATCH ✗ if they differ.

    Parameters:
        original_path (str): Path to the original (sent) file.
        received_path (str): Path to the received (reconstructed) file.
    """
    print("\n" + "=" * 50)
    print("         SHA-256 FILE VERIFICATION")
    print("=" * 50)

    original_hash = sha256_hash(original_path)
    received_hash  = sha256_hash(received_path)

    if original_hash is None or received_hash is None:
        print("[ERROR] Could not compute hash for one or both files.")
        return

    print(f"Original SHA-256 : {original_hash}")
    print(f"Received SHA-256  : {received_hash}")
    print()

    if original_hash == received_hash:
        print("Result: MATCH [OK]  -- Files are IDENTICAL. Transfer was successful!")
    else:
        print("Result: MISMATCH [!!] -- Files are DIFFERENT. Something went wrong!")

    print("=" * 50)


def print_progress(transferred, total, label="Progress"):
    """
    Prints a simple ASCII progress bar on a single line.

    Example output:
        Progress: [####################          ] 67%  (68 KB / 100 KB)

    Parameters:
        transferred (int): Number of bytes transferred so far.
        total       (int): Total number of bytes to transfer.
        label       (str): Label to display before the progress bar.
    """
    if total == 0:
        return  # Avoid division by zero for empty files

    percent = int((transferred / total) * 100)
    bar_filled = percent // 5           # Each '#' represents 5%
    bar_empty  = 20 - bar_filled        # Total bar width is 20 characters

    bar = "#" * bar_filled + " " * bar_empty

    # \r moves the cursor back to the start of the line (overwrites previous output).
    # end="" prevents a newline so the bar updates in place.
    print(f"\r{label}: [{bar}] {percent}%  ({transferred // 1024} KB / {total // 1024} KB)",
          end="", flush=True)

    if transferred >= total:
        print()  # Move to the next line when done


def list_shared_files(shared_folder):
    """
    Lists all files in the shared folder and prints them to the terminal.

    Parameters:
        shared_folder (str): Path to the folder containing shared files.

    Returns:
        files (list of str): List of filenames (not full paths).
    """
    if not os.path.exists(shared_folder):
        print(f"[INFO] Shared folder '{shared_folder}' does not exist. Creating it...")
        os.makedirs(shared_folder)
        print(f"[INFO] Put files you want to share inside the '{shared_folder}' folder.")
        return []

    # os.listdir() returns a list of all filenames in the folder.
    all_entries = os.listdir(shared_folder)

    # Filter out subdirectories — we only want files.
    files = [entry for entry in all_entries
             if os.path.isfile(os.path.join(shared_folder, entry))]

    if not files:
        print(f"[INFO] No files found in '{shared_folder}'. Add some files to share!")
        return []

    print("\n" + "=" * 40)
    print(f"   FILES IN '{shared_folder}/' FOLDER")
    print("=" * 40)
    for i, filename in enumerate(files):
        filepath  = os.path.join(shared_folder, filename)
        file_size = os.path.getsize(filepath)  # Size in bytes
        # Display size in a human-friendly format
        if file_size < 1024:
            size_str = f"{file_size} B"
        elif file_size < 1024 * 1024:
            size_str = f"{file_size // 1024} KB"
        else:
            size_str = f"{file_size // (1024 * 1024)} MB"
        print(f"  {i + 1}. {filename}  ({size_str})")
    print("=" * 40)

    return files
