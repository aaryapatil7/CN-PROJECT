"""
File Chunk Manager.

Handles scanning shared files, dividing files into fixed-size chunks,
generating file metadata manifests, and seeking/reading specific chunk payloads on demand.
"""

import os
import math
from typing import Dict, Any, List, Optional

from .integrity import calculate_file_sha256, calculate_bytes_sha256
from config import DEFAULT_CHUNK_SIZE


class ChunkManager:
    """
    Manages local file indexing, metadata generation, and chunk read operations.
    """

    def __init__(self, shared_dir: str, chunk_size: int = DEFAULT_CHUNK_SIZE):
        self.shared_dir = os.path.abspath(shared_dir)
        self.chunk_size = chunk_size
        self.metadata_cache: Dict[str, Dict[str, Any]] = {}
        os.makedirs(self.shared_dir, exist_ok=True)
        self.refresh_shared_files()

    def refresh_shared_files(self) -> List[str]:
        """
        Scan the shared directory, index all files, and generate metadata manifests.

        Returns:
            List of shared file names.
        """
        file_names = []
        for entry in os.listdir(self.shared_dir):
            full_path = os.path.join(self.shared_dir, entry)
            if os.path.isfile(full_path):
                file_names.append(entry)
                if entry not in self.metadata_cache:
                    self._index_file(entry, full_path)
        return file_names

    def _index_file(self, file_name: str, file_path: str) -> Dict[str, Any]:
        """
        Compute file metadata and chunk manifests.
        """
        file_size = os.path.getsize(file_path)
        file_sha256 = calculate_file_sha256(file_path)
        total_chunks = max(1, math.ceil(file_size / self.chunk_size)) if file_size > 0 else 1

        chunks_meta = []
        with open(file_path, "rb") as f:
            for cid in range(total_chunks):
                f.seek(cid * self.chunk_size)
                chunk_bytes = f.read(self.chunk_size)
                chunks_meta.append({
                    "chunk_id": cid,
                    "size": len(chunk_bytes),
                    "sha256": calculate_bytes_sha256(chunk_bytes),
                })

        manifest = {
            "file_name": file_name,
            "file_size": file_size,
            "chunk_size": self.chunk_size,
            "total_chunks": total_chunks,
            "file_sha256": file_sha256,
            "chunks": chunks_meta,
        }
        self.metadata_cache[file_name] = manifest
        return manifest

    def get_file_metadata(self, file_name: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve metadata manifest for a shared file.
        """
        if file_name in self.metadata_cache:
            return self.metadata_cache[file_name]

        full_path = os.path.join(self.shared_dir, file_name)
        if os.path.isfile(full_path):
            return self._index_file(file_name, full_path)
        return None

    def read_chunk(self, file_name: str, chunk_id: int) -> Optional[bytes]:
        """
        Read a single chunk from disk using file seeking.
        Does NOT load the entire file into memory.

        Args:
            file_name: Name of the file in the shared directory.
            chunk_id: 0-indexed chunk identifier.

        Returns:
            Bytes for the requested chunk, or None if file/chunk is invalid.
        """
        full_path = os.path.join(self.shared_dir, file_name)
        if not os.path.isfile(full_path):
            return None

        file_size = os.path.getsize(full_path)
        offset = chunk_id * self.chunk_size
        if offset >= file_size and file_size > 0:
            return None

        try:
            with open(full_path, "rb") as f:
                f.seek(offset)
                chunk_bytes = f.read(self.chunk_size)
                return chunk_bytes
        except OSError:
            return None

    def list_shared_files(self) -> List[str]:
        """Return list of all locally shared file names."""
        return self.refresh_shared_files()
