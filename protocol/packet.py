"""
Custom Application-Layer Packet for Selective Repeat UDP Transport.

Defines packet types, binary serialization/deserialization with fixed-size
binary header and variable payload, and checksum calculation.
"""

import enum
import json
import struct
from typing import Optional, Dict, Any

from .checksum import calculate_checksum, verify_checksum
from config import MAGIC_COOKIE


class PacketType(enum.IntEnum):
    """Enumeration of Application-Layer Packet Types."""
    DATA = 0x01           # File chunk slice with sequence number
    ACK = 0x02            # Selective acknowledgment for sequence number
    REQUEST = 0x03        # Request for a specific chunk or file transfer
    METADATA_REQ = 0x04   # Request for file manifest & chunk SHA list
    METADATA_RESP = 0x05  # Response containing file manifest JSON
    DISCOVERY = 0x06      # Peer beacon announcement / heartbeat
    PEER_LIST = 0x07      # Active peer list exchange
    FIN = 0x08            # Final packet for chunk/stream completion
    ERROR = 0x09          # Error notification (missing file, invalid chunk)


class Packet:
    """
    Application-Layer Packet for P2P UDP Communication.

    Binary Header Layout (18 bytes, Big-Endian):
    ---------------------------------------------------------------
    | Field          | Size    | Description                       |
    |----------------|---------|-----------------------------------|
    | Magic Cookie   | 2 bytes | Protocol identifier (0x5032)      |
    | Packet Type    | 1 byte  | PacketType enum (DATA, ACK, etc.) |
    | Flags          | 1 byte  | Control flags (0x01=FIN, etc.)    |
    | Sequence No.   | 4 bytes | Sequence number in sliding window |
    | Chunk ID       | 4 bytes | Chunk identifier in file (0..N-1) |
    | Payload Length | 2 bytes | Byte length of following payload  |
    | Checksum       | 4 bytes | CRC32 over header + payload       |
    ---------------------------------------------------------------
    | Payload        | N bytes | Raw binary data or JSON bytes     |
    ---------------------------------------------------------------
    """

    # Header Struct Format: !HBBIIHI (Big-Endian)
    HEADER_FORMAT = "!HBBIIHI"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 18 bytes

    # Flag definitions
    FLAG_NONE = 0x00
    FLAG_FIN = 0x01
    FLAG_RETRANSMIT = 0x02

    def __init__(
        self,
        pkt_type: PacketType,
        seq_num: int = 0,
        chunk_id: int = 0,
        flags: int = FLAG_NONE,
        payload: bytes = b"",
        checksum: Optional[int] = None,
    ):
        self.magic = MAGIC_COOKIE
        self.pkt_type = PacketType(pkt_type)
        self.flags = flags
        self.seq_num = seq_num
        self.chunk_id = chunk_id
        self.payload = payload if isinstance(payload, bytes) else payload.encode("utf-8")
        self.payload_len = len(self.payload)
        self.checksum = checksum if checksum is not None else self._compute_checksum()

    def _compute_checksum(self) -> int:
        """Compute CRC32 checksum over header fields (with checksum=0) + payload."""
        # Pack header with checksum field set to 0
        hdr_without_crc = struct.pack(
            self.HEADER_FORMAT,
            self.magic,
            int(self.pkt_type),
            self.flags,
            self.seq_num,
            self.chunk_id,
            self.payload_len,
            0,
        )
        return calculate_checksum(hdr_without_crc + self.payload)

    def encode(self) -> bytes:
        """Serialize the packet into wire format (Header + Payload)."""
        self.payload_len = len(self.payload)
        self.checksum = self._compute_checksum()
        header = struct.pack(
            self.HEADER_FORMAT,
            self.magic,
            int(self.pkt_type),
            self.flags,
            self.seq_num,
            self.chunk_id,
            self.payload_len,
            self.checksum,
        )
        return header + self.payload

    @classmethod
    def decode(cls, raw_bytes: bytes) -> "Packet":
        """
        Deserialize wire bytes into a Packet object.

        Raises:
            ValueError: If packet is shorter than header, has invalid magic cookie,
                        or fails CRC32 checksum verification.
        """
        if len(raw_bytes) < cls.HEADER_SIZE:
            raise ValueError(f"Packet too short: {len(raw_bytes)} bytes < {cls.HEADER_SIZE} header bytes")

        header_bytes = raw_bytes[:cls.HEADER_SIZE]
        magic, ptype_raw, flags, seq_num, chunk_id, payload_len, wire_crc = struct.unpack(
            cls.HEADER_FORMAT, header_bytes
        )

        if magic != MAGIC_COOKIE:
            raise ValueError(f"Invalid magic cookie: {hex(magic)} (expected {hex(MAGIC_COOKIE)})")

        payload = raw_bytes[cls.HEADER_SIZE : cls.HEADER_SIZE + payload_len]
        if len(payload) != payload_len:
            raise ValueError(f"Payload length mismatch: expected {payload_len}, got {len(payload)}")

        # Verify CRC32 checksum
        hdr_zero_crc = struct.pack(
            cls.HEADER_FORMAT,
            magic,
            ptype_raw,
            flags,
            seq_num,
            chunk_id,
            payload_len,
            0,
        )
        if not verify_checksum(hdr_zero_crc + payload, wire_crc):
            raise ValueError(f"Checksum verification failed for Seq={seq_num}, Chunk={chunk_id}")

        return cls(
            pkt_type=PacketType(ptype_raw),
            seq_num=seq_num,
            chunk_id=chunk_id,
            flags=flags,
            payload=payload,
            checksum=wire_crc,
        )

    def payload_as_json(self) -> Dict[str, Any]:
        """Decode payload as a JSON dictionary."""
        return json.loads(self.payload.decode("utf-8"))

    # --- Convenience Factory Methods ---

    @classmethod
    def create_data(cls, seq_num: int, chunk_id: int, payload: bytes, is_fin: bool = False) -> "Packet":
        flags = cls.FLAG_FIN if is_fin else cls.FLAG_NONE
        return cls(PacketType.DATA, seq_num=seq_num, chunk_id=chunk_id, flags=flags, payload=payload)

    @classmethod
    def create_ack(cls, seq_num: int, chunk_id: int) -> "Packet":
        return cls(PacketType.ACK, seq_num=seq_num, chunk_id=chunk_id)

    @classmethod
    def create_request(cls, file_name: str, chunk_id: int) -> "Packet":
        payload = json.dumps({"file_name": file_name, "chunk_id": chunk_id}).encode("utf-8")
        return cls(PacketType.REQUEST, chunk_id=chunk_id, payload=payload)

    @classmethod
    def create_metadata_req(cls, file_name: str) -> "Packet":
        payload = json.dumps({"file_name": file_name}).encode("utf-8")
        return cls(PacketType.METADATA_REQ, payload=payload)

    @classmethod
    def create_metadata_resp(cls, metadata: Dict[str, Any]) -> "Packet":
        payload = json.dumps(metadata).encode("utf-8")
        return cls(PacketType.METADATA_RESP, payload=payload)

    @classmethod
    def create_discovery(cls, peer_id: str, port: int, shared_files: list) -> "Packet":
        payload = json.dumps({
            "peer_id": peer_id,
            "port": port,
            "shared_files": shared_files,
        }).encode("utf-8")
        return cls(PacketType.DISCOVERY, payload=payload)

    @classmethod
    def create_fin(cls, chunk_id: int, total_packets: int) -> "Packet":
        payload = json.dumps({"chunk_id": chunk_id, "total_packets": total_packets}).encode("utf-8")
        return cls(PacketType.FIN, chunk_id=chunk_id, flags=cls.FLAG_FIN, payload=payload)

    @classmethod
    def create_error(cls, message: str, chunk_id: int = 0) -> "Packet":
        payload = json.dumps({"error": message}).encode("utf-8")
        return cls(PacketType.ERROR, chunk_id=chunk_id, payload=payload)

    def __repr__(self) -> str:
        return (
            f"<Packet type={self.pkt_type.name} seq={self.seq_num} "
            f"chunk={self.chunk_id} len={self.payload_len} flags={hex(self.flags)}>"
        )
