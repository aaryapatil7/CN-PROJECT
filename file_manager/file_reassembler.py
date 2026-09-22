"""
File Reassembly and Resume Manager.

Stores incoming chunk parts temporarily, tracks partial download progress,
supports resuming interrupted transfers, and stitches chunks into final verified files.
"""

import os
import shutil
from typing import List, Tuple, Set

from .integrity import calculate_file_sha256, verify_file_sha256
from config import DOWNLOADS_DIR


class FileReassembler:
    """
    Manages temporary chunk storage, download resumption, and sequential file reassembly.
    """

    def __init__(self, downloads_dir: str = DOWNLOADS_DIR):
        self.downloads_dir = os.path.abspath(downloads_dir)
        os.makedirs(self.downloads_dir, exist_ok=True)

    def _get_part_dir(self, file_name: str) -> str:
        """Return path to temporary staging directory for a file download."""
        part_name = f"{file_name}.part"
        part_path = os.path.join(self.downloads_dir, part_name)
        os.makedirs(part_path, exist_ok=True)
        return part_path

    def _chunk_file_path(self, file_name: str, chunk_id: int) -> str:
        """Return filename for an individual chunk part file."""
        part_dir = self._get_part_dir(file_name)
        return os.path.join(part_dir, f"chunk_{chunk_id:05d}")

    def save_chunk(self, file_name: str, chunk_id: int, data: bytes) -> str:
        """
        Save a received chunk payload to disk in the temporary staging directory.

        Args:
            file_name: Name of the target file.
            chunk_id: Chunk sequence identifier.
            data: Chunk byte content.

        Returns:
            Path to saved chunk file.
        """
        chunk_path = self._chunk_file_path(file_name, chunk_id)
        with open(chunk_path, "wb") as f:
            f.write(data)
        return chunk_path

    def has_chunk(self, file_name: str, chunk_id: int) -> bool:
        """Check if a specific chunk has already been downloaded."""
        chunk_path = self._chunk_file_path(file_name, chunk_id)
        return os.path.isfile(chunk_path) and os.path.getsize(chunk_path) > 0

    def get_downloaded_chunks(self, file_name: str) -> Set[int]:
        """
        Scan staging directory and return set of chunk IDs already present.
        """
        part_dir = self._get_part_dir(file_name)
        downloaded = set()
        if not os.path.isdir(part_dir):
            return downloaded

        for entry in os.listdir(part_dir):
            if entry.startswith("chunk_"):
                try:
                    cid = int(entry.split("_")[1])
                    if os.path.getsize(os.path.join(part_dir, entry)) >= 0:
                        downloaded.add(cid)
                except (IndexError, ValueError):
                    continue
        return downloaded

    def get_missing_chunks(self, file_name: str, total_chunks: int) -> List[int]:
        """
        Determine which chunk IDs are still missing for resume capability.

        Args:
            file_name: Name of the file being downloaded.
            total_chunks: Expected total chunk count from metadata manifest.

        Returns:
            List of missing chunk IDs sorted in ascending order.
        """
        downloaded = self.get_downloaded_chunks(file_name)
        missing = [cid for cid in range(total_chunks) if cid not in downloaded]
        return missing

    def reassemble_file(
        self,
        file_name: str,
        total_chunks: int,
        expected_sha256: str,
        destination_dir: str = None,
    ) -> Tuple[bool, str, str]:
        """
        Stitch all chunk parts in order, write the final completed file,
        and verify cryptographic SHA-256 integrity.

        Args:
            file_name: Name of the target file.
            total_chunks: Total number of chunk files expected.
            expected_sha256: Original file SHA-256 hash.
            destination_dir: Directory where final file should be placed.

        Returns:
            Tuple of (success: bool, status_message: str, final_file_path: str).
        """
        dest_dir = destination_dir or self.downloads_dir
        os.makedirs(dest_dir, exist_ok=True)
        final_path = os.path.join(dest_dir, file_name)
        part_dir = self._get_part_dir(file_name)

        # Check that all chunks exist
        missing = self.get_missing_chunks(file_name, total_chunks)
        if missing:
            return False, f"Missing chunks: {missing[:10]} (total missing: {len(missing)})", ""

        # Write sequential chunks into target file
        temp_assembled = final_path + ".tmp_assembling"
        try:
            with open(temp_assembled, "wb") as outfile:
                for cid in range(total_chunks):
                    chunk_path = self._chunk_file_path(file_name, cid)
                    with open(chunk_path, "rb") as infile:
                        shutil.copyfileobj(infile, outfile)

            # Calculate SHA-256 of assembled file
            actual_sha256 = calculate_file_sha256(temp_assembled)
            if actual_sha256.lower() != expected_sha256.lower():
                if os.path.exists(temp_assembled):
                    os.remove(temp_assembled)
                return False, f"SHA-256 verification failed! Expected: {expected_sha256}, Got: {actual_sha256}", ""

            # Atomically rename to final destination file
            if os.path.exists(final_path):
                os.remove(final_path)
            os.rename(temp_assembled, final_path)

            # Clean up temporary chunk files
            try:
                shutil.rmtree(part_dir)
            except OSError:
                pass

            return True, f"File integrity verified (SHA-256: {actual_sha256[:16]}...)", final_path
        except Exception as e:
            if os.path.exists(temp_assembled):
                os.remove(temp_assembled)
            return False, f"Reassembly error: {str(e)}", ""
