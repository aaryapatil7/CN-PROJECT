"""
========================================================================================
DECENTRALIZED MULTI-THREADED P2P FILE-SHARING APPLICATION (SELECTIVE REPEAT OVER UDP)
========================================================================================
All-In-One Self-Contained Implementation for Computer Networks Academic Evaluation.

Components Implemented:
1. Component 1 (15 Marks): Decentralized multi-threaded P2P chunk downloader over UDP.
2. Component 2 (15 Marks): Custom Application-Layer Selective Repeat Sliding Window Protocol
   over UDP with simulated packet loss, CRC-32 checksums, per-packet timers, and out-of-order buffering.
========================================================================================
"""

import os
import sys
import time
import math
import zlib
import json
import struct
import random
import socket
import queue
import hashlib
import threading
from typing import Dict, List, Tuple, Any, Optional, Set

# --------------------------------------------------------------------------------------
# 1. PROTOCOL CONSTANTS & CONFIGURATION
# --------------------------------------------------------------------------------------
MAGIC_COOKIE = 0x5032       # 2-byte magic identifier ('P2')
HEADER_FORMAT = "!HBBIIHI"  # 18-byte binary struct (Big-Endian)
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 18 bytes
MAX_PAYLOAD = 1400          # 1400 bytes payload (fits Ethernet MTU)
CHUNK_SIZE = 64 * 1024      # 64 KB chunk size
DEFAULT_WINDOW_SIZE = 8     # Default Selective Repeat sliding window size
DEFAULT_TIMEOUT = 0.8       # Per-packet retransmission timeout (seconds)

# Packet Type Constants
PKT_DATA = 0x01
PKT_ACK = 0x02
PKT_REQ = 0x03
PKT_META_REQ = 0x04
PKT_META_RESP = 0x05
PKT_DISC = 0x06
PKT_FIN = 0x07
PKT_ERR = 0x08

# Default known peer IP addresses for multi-computer deployment
KNOWN_PEER_IPS = [
    "10.30.164.22",
    "10.30.164.23",
    "10.30.164.24",
]

PEER_IP_MAP = {
    "10.30.164.22": "PEER_A",
    "10.30.164.23": "PEER_B",
    "10.30.164.24": "PEER_C",
}


def get_local_ip() -> str:
    """Detect local LAN IPv4 address of this machine."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
    except Exception:
        try:
            ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            ip = "127.0.0.1"
    finally:
        s.close()
    return ip


def get_broadcast_addresses() -> list:
    """Return standard and subnet-directed broadcast addresses."""
    addrs = ["<broadcast>", "255.255.255.255"]
    local_ip = get_local_ip()
    if "." in local_ip and local_ip != "127.0.0.1":
        prefix = local_ip.rsplit(".", 1)[0]
        addrs.append(f"{prefix}.255")
    return list(dict.fromkeys(addrs))


# --------------------------------------------------------------------------------------
# 2. CUSTOM APPLICATION-LAYER PACKET (BINARY HEADER + CRC-32)
# --------------------------------------------------------------------------------------
class Packet:
    """
    18-Byte Binary Header Layout:
    | Magic(2B) | Type(1B) | Flags(1B) | SeqNum(4B) | ChunkID(4B) | Length(2B) | CRC32(4B) | Payload(NB) |
    """
    def __init__(self, pkt_type: int, seq_num: int = 0, chunk_id: int = 0, flags: int = 0, payload: bytes = b""):
        self.magic = MAGIC_COOKIE
        self.pkt_type = pkt_type
        self.flags = flags
        self.seq_num = seq_num
        self.chunk_id = chunk_id
        self.payload = payload if isinstance(payload, bytes) else payload.encode("utf-8")
        self.length = len(self.payload)
        self.crc = self._compute_crc()

    def _compute_crc(self) -> int:
        hdr_zero_crc = struct.pack(HEADER_FORMAT, self.magic, self.pkt_type, self.flags, self.seq_num, self.chunk_id, self.length, 0)
        return zlib.crc32(hdr_zero_crc + self.payload) & 0xFFFFFFFF

    def encode(self) -> bytes:
        self.length = len(self.payload)
        self.crc = self._compute_crc()
        hdr = struct.pack(HEADER_FORMAT, self.magic, self.pkt_type, self.flags, self.seq_num, self.chunk_id, self.length, self.crc)
        return hdr + self.payload

    @classmethod
    def decode(cls, raw: bytes) -> "Packet":
        if len(raw) < HEADER_SIZE:
            raise ValueError("Packet shorter than 18-byte header")
        magic, ptype, flags, seq, cid, plen, wire_crc = struct.unpack(HEADER_FORMAT, raw[:HEADER_SIZE])
        if magic != MAGIC_COOKIE:
            raise ValueError(f"Invalid magic cookie: {hex(magic)}")
        payload = raw[HEADER_SIZE : HEADER_SIZE + plen]
        # Verify CRC32
        hdr_zero_crc = struct.pack(HEADER_FORMAT, magic, ptype, flags, seq, cid, plen, 0)
        if (zlib.crc32(hdr_zero_crc + payload) & 0xFFFFFFFF) != wire_crc:
            raise ValueError(f"CRC-32 Checksum error for Seq={seq}, Chunk={cid}")
        pkt = cls(ptype, seq, cid, flags, payload)
        pkt.crc = wire_crc
        return pkt


# --------------------------------------------------------------------------------------
# 3. SELECTIVE REPEAT SENDER (WINDOW + TIMERS + SELECTIVE RETRANSMIT)
# --------------------------------------------------------------------------------------
class SRSender:
    """Selective Repeat Sender: transmits chunk with sliding window and individual packet timers."""
    def __init__(self, sock: socket.socket, dest_addr: Tuple[str, int], chunk_id: int, data: bytes,
                 window_size: int = DEFAULT_WINDOW_SIZE, timeout: float = DEFAULT_TIMEOUT, loss_prob: float = 0.0):
        self.sock = sock
        self.dest_addr = dest_addr
        self.chunk_id = chunk_id
        self.data = data
        self.window_size = window_size
        self.timeout = timeout
        self.loss_prob = loss_prob

        # Slice data into packets of MAX_PAYLOAD bytes
        self.packets = []
        for i in range(0, max(1, len(data)), MAX_PAYLOAD):
            slice_data = data[i : i + MAX_PAYLOAD]
            is_fin = (i + MAX_PAYLOAD >= len(data))
            flags = 0x01 if is_fin else 0x00
            self.packets.append(Packet(PKT_DATA, seq_num=len(self.packets), chunk_id=chunk_id, flags=flags, payload=slice_data))
        self.total_packets = len(self.packets)

        self.send_base = 0
        self.next_seq_num = 0
        self.acked_seqs: Set[int] = set()
        self.timers: Dict[int, float] = {}
        self.lock = threading.Lock()
        self.finished = threading.Event()

    def handle_ack(self, ack_pkt: Packet):
        if ack_pkt.chunk_id != self.chunk_id:
            return
        seq = ack_pkt.seq_num
        with self.lock:
            if seq not in self.acked_seqs:
                self.acked_seqs.add(seq)
                self.timers.pop(seq, None)
                # Slide window forward past all consecutively ACKed packets
                if seq == self.send_base:
                    while self.send_base < self.total_packets and self.send_base in self.acked_seqs:
                        self.send_base += 1
            if len(self.acked_seqs) == self.total_packets:
                self.finished.set()

    def _send_pkt(self, pkt: Packet, is_retransmit: bool = False):
        # Simulated Packet Loss
        if self.loss_prob > 0.0 and random.random() < self.loss_prob:
            print(f"  \033[91m[LOSS] Dropped DATA Seq={pkt.seq_num} Chunk={pkt.chunk_id} {'(Retransmit)' if is_retransmit else ''}\033[0m")
            return
        try:
            self.sock.sendto(pkt.encode(), self.dest_addr)
            if is_retransmit:
                print(f"  \033[95m[RETRANSMIT] Sent DATA Seq={pkt.seq_num} Chunk={pkt.chunk_id}\033[0m")
            else:
                print(f"  [DATA] Sent Seq={pkt.seq_num} Chunk={pkt.chunk_id} ({pkt.length}B)")
        except socket.error as e:
            print(f"  [ERROR] Socket sendto error: {e}")

    def send_chunk(self) -> bool:
        while not self.finished.is_set():
            with self.lock:
                # 1. Send packets within sliding window [send_base, send_base + window_size - 1]
                while self.next_seq_num < self.send_base + self.window_size and self.next_seq_num < self.total_packets:
                    pkt = self.packets[self.next_seq_num]
                    self.timers[self.next_seq_num] = time.time()
                    self._send_pkt(pkt, is_retransmit=False)
                    self.next_seq_num += 1

                # 2. Check individual timers for unacknowledged packets
                now = time.time()
                for seq in range(self.send_base, self.next_seq_num):
                    if seq not in self.acked_seqs and seq in self.timers:
                        if now - self.timers[seq] > self.timeout:
                            print(f"  \033[93m[TIMEOUT] Timer expired for Seq={seq} (resending ONLY Seq={seq})\033[0m")
                            self.timers[seq] = now
                            self._send_pkt(self.packets[seq], is_retransmit=True)

            if self.finished.is_set():
                break
            time.sleep(0.02)

        # Send FIN packet
        fin = Packet(PKT_FIN, chunk_id=self.chunk_id, payload=json.dumps({"total_packets": self.total_packets}).encode("utf-8"))
        try:
            self.sock.sendto(fin.encode(), self.dest_addr)
        except socket.error:
            pass
        return True


# ----------------------------------------------------------------------
# 4. SELECTIVE REPEAT RECEIVER (OUT-OF-ORDER BUFFERING & ACKs)
# ----------------------------------------------------------------------
class SRReceiver:
    """Selective Repeat Receiver: buffers out-of-order packets and delivers in-order."""
    def __init__(self, sock: socket.socket, sender_addr: Tuple[str, int], chunk_id: int, window_size: int = DEFAULT_WINDOW_SIZE):
        self.sock = sock
        self.sender_addr = sender_addr
        self.chunk_id = chunk_id
        self.window_size = window_size
        self.rcv_base = 0
        self.buffer: Dict[int, bytes] = {}          # Out-of-order buffer: seq -> payload
        self.received_slices: Dict[int, bytes] = {}  # Delivered in-order: seq -> payload
        self.total_packets: Optional[int] = None
        self.lock = threading.Lock()
        self.finished = threading.Event()

    def handle_packet(self, pkt: Packet):
        if pkt.chunk_id != self.chunk_id:
            return
        with self.lock:
            if pkt.pkt_type == PKT_FIN:
                try:
                    data = json.loads(pkt.payload.decode("utf-8"))
                    self.total_packets = data.get("total_packets", self.rcv_base)
                except Exception:
                    self.total_packets = self.rcv_base
                self._check_done()
                return

            if pkt.pkt_type != PKT_DATA:
                return

            seq = pkt.seq_num
            # Always send ACK for packets in window
            ack = Packet(PKT_ACK, seq_num=seq, chunk_id=self.chunk_id)
            try:
                self.sock.sendto(ack.encode(), self.sender_addr)
                print(f"  \033[92m[ACK] Sent ACK={seq} for Chunk {self.chunk_id}\033[0m")
            except socket.error:
                pass

            if pkt.flags & 0x01:
                self.total_packets = seq + 1

            # Case 1: Packet within receive window [rcv_base, rcv_base + window_size - 1]
            if self.rcv_base <= seq < self.rcv_base + self.window_size:
                if seq == self.rcv_base:
                    # In-order packet arrived!
                    self.received_slices[seq] = pkt.payload
                    self.rcv_base += 1
                    # Flush consecutive buffered packets
                    while self.rcv_base in self.buffer:
                        self.received_slices[self.rcv_base] = self.buffer.pop(self.rcv_base)
                        print(f"  \033[96m[BUFFER] Delivering buffered packet Seq={self.rcv_base}\033[0m")
                        self.rcv_base += 1
                    self._check_done()
                else:
                    # Out-of-order packet: Buffer it!
                    if seq not in self.buffer:
                        self.buffer[seq] = pkt.payload
                        print(f"  \033[93m[BUFFER] Stored out-of-order Seq={seq} (waiting for Seq={self.rcv_base})\033[0m")

    def _check_done(self):
        if self.total_packets is not None and self.rcv_base >= self.total_packets:
            self.finished.set()

    def get_data(self) -> bytes:
        total = self.total_packets if self.total_packets is not None else self.rcv_base
        return b"".join(self.received_slices.get(i, b"") for i in range(total))


# --------------------------------------------------------------------------------------
# 5. PEER NODE & MULTI-THREADED DOWNLOADER
# --------------------------------------------------------------------------------------
class SimplePeer:
    """Core Peer Node combining Selective Repeat UDP transport and multi-peer downloading."""
    def __init__(self, peer_id: str, port: int, shared_dir: str, downloads_dir: str, loss_prob: float = 0.0, known_peer_ips: Optional[List[str]] = None):
        self.peer_id = peer_id
        self.host = "0.0.0.0"
        self.port = port
        self.shared_dir = shared_dir
        self.downloads_dir = downloads_dir
        self.loss_prob = loss_prob
        self.known_peer_ips = list(known_peer_ips) if known_peer_ips is not None else list(KNOWN_PEER_IPS)
        self.candidate_ports = [5001, 5002, 5003, 5004, 5005]
        self.local_ip = get_local_ip()

        os.makedirs(self.shared_dir, exist_ok=True)
        os.makedirs(self.downloads_dir, exist_ok=True)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        except OSError:
            pass
        if os.name == "nt" and hasattr(socket, "SIO_UDP_CONNRESET"):
            try:
                self.sock.ioctl(socket.SIO_UDP_CONNRESET, False)
            except Exception:
                pass
        self.sock.bind((self.host, self.port))

        self.peers: Dict[str, Dict[str, Any]] = {}   # peer_id -> {ip, port, files, last_seen}
        self.active_senders: Dict[Tuple[Tuple[str, int], int], SRSender] = {}
        self.active_receivers: Dict[Tuple[Tuple[str, int], int], SRReceiver] = {}
        self.meta_responses: Dict[str, Dict[str, Any]] = {}
        self.meta_events: Dict[str, threading.Event] = {}
        self.lock = threading.Lock()
        self.running = True

        # Start background receiver and discovery beacon threads
        threading.Thread(target=self._recv_loop, daemon=True).start()
        threading.Thread(target=self._beacon_loop, daemon=True).start()

    def list_shared_files(self) -> List[str]:
        return [f for f in os.listdir(self.shared_dir) if os.path.isfile(os.path.join(self.shared_dir, f))]

    def _beacon_loop(self):
        """Broadcast periodic discovery announcements to find other peers across LAN and local ports."""
        while self.running:
            disc = Packet(PKT_DISC, payload=json.dumps({
                "peer_id": self.peer_id, "port": self.port, "files": self.list_shared_files()
            }).encode("utf-8"))
            raw = disc.encode()

            # 1. Ping configured known peer IPs across candidate ports (LAN seeds)
            target_ips = set(self.known_peer_ips)
            target_ips.add("127.0.0.1")
            for target_ip in target_ips:
                for target_port in self.candidate_ports:
                    if (target_ip in ("127.0.0.1", "localhost", self.local_ip)) and target_port == self.port:
                        continue
                    try:
                        self.sock.sendto(raw, (target_ip, target_port))
                    except OSError:
                        pass

            # 2. Ping known peers in table
            with self.lock:
                known_endpoints = [(p["ip"], p["port"]) for p in self.peers.values()]
            for ip, pport in known_endpoints:
                try:
                    self.sock.sendto(raw, (ip, pport))
                except OSError:
                    pass

            # 3. Broadcast to subnet and global broadcast
            for b_ip in get_broadcast_addresses():
                for target_port in self.candidate_ports:
                    try:
                        self.sock.sendto(raw, (b_ip, target_port))
                    except OSError:
                        pass

            time.sleep(1.5)

    def _recv_loop(self):
        """Main UDP socket listener."""
        while self.running:
            try:
                raw, src = self.sock.recvfrom(65535)
                pkt = Packet.decode(raw)
            except (socket.timeout, ConnectionResetError):
                continue
            except OSError as e:
                if getattr(e, "winerror", None) == 10054 or getattr(e, "errno", None) == 10054:
                    continue
                break
            except Exception:
                continue

            # 1. Discovery Packet
            if pkt.pkt_type == PKT_DISC:
                info = json.loads(pkt.payload.decode("utf-8"))
                pid = info["peer_id"]
                if pid != self.peer_id:
                    is_new = pid not in self.peers
                    self.peers[pid] = {"ip": src[0], "port": info["port"], "files": info["files"], "last_seen": time.time()}
                    if is_new:
                        try:
                            reply_pkt = Packet(PKT_DISC, payload=json.dumps({
                                "peer_id": self.peer_id, "port": self.port, "files": self.list_shared_files()
                            }).encode("utf-8"))
                            self.sock.sendto(reply_pkt.encode(), (src[0], info["port"]))
                        except Exception:
                            pass

            # 2. Metadata Request (Peer wants file details)
            elif pkt.pkt_type == PKT_META_REQ:
                fname = json.loads(pkt.payload.decode("utf-8"))["file_name"]
                fpath = os.path.join(self.shared_dir, fname)
                if os.path.isfile(fpath):
                    fsize = os.path.getsize(fpath)
                    sha = hashlib.sha256(open(fpath, "rb").read()).hexdigest()
                    total_chunks = max(1, math.ceil(fsize / CHUNK_SIZE))
                    resp = Packet(PKT_META_RESP, payload=json.dumps({
                        "file_name": fname, "file_size": fsize, "total_chunks": total_chunks, "sha256": sha
                    }).encode("utf-8"))
                    self.sock.sendto(resp.encode(), src)

            # 3. Metadata Response
            elif pkt.pkt_type == PKT_META_RESP:
                meta = json.loads(pkt.payload.decode("utf-8"))
                fname = meta["file_name"]
                self.meta_responses[fname] = meta
                if fname in self.meta_events:
                    self.meta_events[fname].set()

            # 4. Chunk Request (We act as Seeder/Sender)
            elif pkt.pkt_type == PKT_REQ:
                req = json.loads(pkt.payload.decode("utf-8"))
                fname = req["file_name"]
                cid = req["chunk_id"]
                fpath = os.path.join(self.shared_dir, fname)
                if os.path.isfile(fpath):
                    with open(fpath, "rb") as f:
                        f.seek(cid * CHUNK_SIZE)
                        chunk_bytes = f.read(CHUNK_SIZE)
                    # Start Selective Repeat Sender
                    sender = SRSender(self.sock, src, cid, chunk_bytes, loss_prob=self.loss_prob)
                    with self.lock:
                        self.active_senders[(src, cid)] = sender
                    threading.Thread(target=sender.send_chunk, daemon=True).start()

            # 5. Selective Repeat ACK
            elif pkt.pkt_type == PKT_ACK:
                with self.lock:
                    sender = self.active_senders.get((src, pkt.chunk_id))
                if sender:
                    sender.handle_ack(pkt)

            # 6. DATA or FIN packet (We act as Receiver/Downloader)
            elif pkt.pkt_type in (PKT_DATA, PKT_FIN):
                with self.lock:
                    receiver = self.active_receivers.get((src, pkt.chunk_id))
                if receiver:
                    receiver.handle_packet(pkt)

    def download_file(self, file_name: str) -> bool:
        """Download file concurrently from multiple peers holding it."""
        seeders = [info for pid, info in self.peers.items() if file_name in info.get("files", [])]
        if not seeders:
            # Send immediate discovery ping to LAN seeds and candidate ports
            disc = Packet(PKT_DISC, payload=json.dumps({"peer_id": self.peer_id, "port": self.port, "files": self.list_shared_files()}).encode("utf-8"))
            raw = disc.encode()
            target_ips = set(self.known_peer_ips)
            target_ips.add("127.0.0.1")
            for target_ip in target_ips:
                for p in self.candidate_ports:
                    if (target_ip in ("127.0.0.1", "localhost", self.local_ip)) and p == self.port:
                        continue
                    try:
                        self.sock.sendto(raw, (target_ip, p))
                    except OSError:
                        pass
            time.sleep(0.8)
            seeders = [info for pid, info in self.peers.items() if file_name in info.get("files", [])]

        if not seeders:
            print(f"\n\033[91m[!] No peers found with file '{file_name}'. Ensure Peer A and Peer C are running.\033[0m")
            return False

        # Request metadata from seeder with retry
        meta = None
        for attempt in range(1, 4):
            for seeder in seeders:
                ev = threading.Event()
                self.meta_events[file_name] = ev
                req_meta = Packet(PKT_META_REQ, payload=json.dumps({"file_name": file_name}).encode("utf-8"))
                self.sock.sendto(req_meta.encode(), (seeder["ip"], seeder["port"]))
                if ev.wait(timeout=2.0):
                    meta = self.meta_responses.get(file_name)
                    if meta:
                        break
            if meta:
                break
            time.sleep(0.3)

        if not meta:
            print("\n\033[91m[!] Failed to get file metadata from seeder. Please retry.\033[0m")
            return False

        meta = self.meta_responses[file_name]
        total_chunks = meta["total_chunks"]
        expected_sha = meta["sha256"]

        print(f"\n\033[96m--> Starting Multi-Threaded Download: '{file_name}' ({meta['file_size']/1024:.1f} KB, {total_chunks} chunks)")
        print(f"    Available Seeders: {[p['port'] for p in seeders]} | Packet Loss: {self.loss_prob*100:.0f}%\033[0m\n")

        chunk_q = queue.Queue()
        for cid in range(total_chunks):
            chunk_q.put(cid)

        downloaded_chunks: Dict[int, bytes] = {}
        q_lock = threading.Lock()
        start_time = time.time()

        def worker(worker_id: int):
            while not chunk_q.empty():
                try:
                    cid = chunk_q.get_nowait()
                except queue.Empty:
                    break

                target_seeder = seeders[worker_id % len(seeders)]
                dest = (target_seeder["ip"], target_seeder["port"])
                print(f"  \033[94m[THREAD-{worker_id}] Requesting Chunk {cid} from {dest[0]}:{dest[1]}\033[0m")

                receiver = SRReceiver(self.sock, dest, cid)
                with self.lock:
                    self.active_receivers[(dest, cid)] = receiver

                req = Packet(PKT_REQ, payload=json.dumps({"file_name": file_name, "chunk_id": cid}).encode("utf-8"))
                self.sock.sendto(req.encode(), dest)

                if receiver.finished.wait(timeout=15.0):
                    data = receiver.get_data()
                    with q_lock:
                        downloaded_chunks[cid] = data
                    print(f"  \033[92m[DONE] Chunk {cid}/{total_chunks-1} downloaded ({len(downloaded_chunks)}/{total_chunks})\033[0m")
                    chunk_q.task_done()
                else:
                    print(f"  \033[91m[RETRY] Chunk {cid} timed out. Re-queueing...\033[0m")
                    chunk_q.task_done()
                    chunk_q.put(cid)
                    time.sleep(0.5)

        # Spawn 2 concurrent worker threads
        threads = []
        for i in range(min(4, len(seeders) * 2)):
            t = threading.Thread(target=worker, args=(i + 1,), daemon=True)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        if len(downloaded_chunks) != total_chunks:
            print("\n\033[91m[!] Download failed: missing chunks.\033[0m")
            return False

        # Reassemble File
        final_path = os.path.join(self.downloads_dir, file_name)
        with open(final_path, "wb") as f:
            for cid in range(total_chunks):
                f.write(downloaded_chunks[cid])

        elapsed = max(0.001, time.time() - start_time)
        actual_sha = hashlib.sha256(open(final_path, "rb").read()).hexdigest()

        print(f"\n\033[92m===============================================================")
        print(f"                   DOWNLOAD COMPLETE                          ")
        print(f"===============================================================\033[0m")
        print(f"  File Name    : {file_name}")
        print(f"  File Size    : {meta['file_size']} bytes")
        print(f"  Time Taken   : {elapsed:.2f} seconds")
        print(f"  Throughput   : {(meta['file_size']/1024)/elapsed:.2f} KB/s")
        print(f"  Expected SHA : {expected_sha}")
        print(f"  Computed SHA : {actual_sha}")
        if actual_sha.lower() == expected_sha.lower():
            print(f"  \033[92mSTATUS       : SHA-256 INTEGRITY VERIFIED (MATCH) [OK]\033[0m")
            print(f"  Saved to     : {final_path}")
            return True
        else:
            print(f"  \033[91mSTATUS       : INTEGRITY CHECK FAILED (MISMATCH)\033[0m")
            return False


# --------------------------------------------------------------------------------------
# 6. MAIN USER INTERACTION MENU
# --------------------------------------------------------------------------------------
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Simple P2P File Sharing (Selective Repeat over UDP)")
    parser.add_argument("--peer-id", type=str, default="", help="Peer ID (e.g. PEER_A, PEER_B, PEER_C)")
    parser.add_argument("--port", type=int, default=5001, help="UDP port (e.g. 5001, 5002, 5003)")
    parser.add_argument("--peers", type=str, default="", help="Comma-separated peer IPs (e.g. 10.30.164.22,10.30.164.23,10.30.164.24)")
    parser.add_argument("--loss", type=float, default=0.0, help="Packet loss probability (0.0 to 0.5)")
    args = parser.parse_args()

    local_ip = get_local_ip()
    auto_peer_id = PEER_IP_MAP.get(local_ip, "")
    peer_id = args.peer_id or auto_peer_id or f"PEER_{args.port}"
    known_peer_ips = [ip.strip() for ip in args.peers.split(",") if ip.strip()] if args.peers else list(KNOWN_PEER_IPS)

    shared = os.path.join("shared_files", peer_id.lower())
    down = os.path.join("downloads", peer_id.lower())

    peer = SimplePeer(peer_id, args.port, shared, down, loss_prob=args.loss, known_peer_ips=known_peer_ips)

    while True:
        try:
            print(f"\n\033[96m===============================================================")
            print(f"  P2P FILE SHARING - {peer_id} | LAN IP: {local_ip}:{args.port} | Loss: {peer.loss_prob*100:.0f}%")
            print(f"===============================================================\033[0m")
            print("  [1] List Active Peers & Discovered Files")
            print("  [2] Download a File from Peers (Multi-Threaded)")
            print("  [3] Set Simulated Packet Loss %")
            print("  [4] Exit")

            choice = input("\nEnter choice [1-4] > ").strip()
            if choice == "1":
                print("\n[ ACTIVE CONNECTED PEERS ]")
                for pid, info in peer.peers.items():
                    print(f"  * {pid} ({info['ip']}:{info['port']}) -> Files: {info['files']}")
                if not peer.peers:
                    print("  (No peers detected yet. Searching...)")
                input("\nPress Enter to continue...")

            elif choice == "2":
                all_files = set()
                for info in peer.peers.values():
                    all_files.update(info.get("files", []))
                print(f"\nAvailable Network Files: {list(all_files)}")
                if not all_files:
                    print("No remote files available to download.")
                    continue
                fname = input("Enter file name to download > ").strip()
                if fname:
                    peer.download_file(fname)
                    input("\nPress Enter to continue...")

            elif choice == "3":
                try:
                    pct = float(input("Enter simulated loss % (0 to 50) > ").strip())
                    peer.loss_prob = max(0.0, min(0.5, pct / 100.0))
                    print(f"Packet loss updated to {peer.loss_prob*100:.0f}%")
                except ValueError:
                    print("Invalid input.")

            elif choice == "4":
                print("Exiting...")
                peer.running = False
                break
        except KeyboardInterrupt:
            peer.running = False
            break


if __name__ == "__main__":
    main()
