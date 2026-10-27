"""UDP file transfer using an application-layer Selective Repeat protocol.

The receiver is a permanent background listener.  It can accept multiple
transfers at the same time, identified by a transfer_id.  The sender uses a
sliding window and retransmits only packets whose ACK has not arrived before
their individual timeout.
"""
import os
import random
import socket
import struct
import threading
import time

import file_utils
import config

# Data packet: transfer_id, sequence_number, total_packets, is_last
HEADER_FORMAT = "!QII B"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
# ACK: transfer_id, sequence_number, type
ACK_FORMAT = "!QIB"
ACK_SIZE = struct.calcsize(ACK_FORMAT)

ACK_TYPE_REGULAR = 0xAA
ACK_TYPE_DONE = 0xFF

# UDP request: magic, filename length, receiver UDP port
REQUEST_MAGIC = 0xF00DBAAD
REQUEST_HEADER_FORMAT = "!IIH"
REQUEST_HEADER_SIZE = struct.calcsize(REQUEST_HEADER_FORMAT)
REQUEST_ACK = b"P2P-UDP-REQUEST-OK"


def _make_packet(transfer_id, seq_no, total_packets, data):
    is_last = 1 if seq_no == total_packets - 1 else 0
    return struct.pack(HEADER_FORMAT, transfer_id, seq_no, total_packets, is_last) + data


def _make_ack(transfer_id, seq_no, ack_type=ACK_TYPE_REGULAR):
    return struct.pack(ACK_FORMAT, transfer_id, seq_no, ack_type)


def _parse_packet(raw):
    if len(raw) < HEADER_SIZE:
        return None
    transfer_id, seq_no, total_packets, is_last = struct.unpack(
        HEADER_FORMAT, raw[:HEADER_SIZE]
    )
    return transfer_id, seq_no, total_packets, is_last, raw[HEADER_SIZE:]


def _parse_ack(raw):
    if len(raw) != ACK_SIZE:
        return None
    return struct.unpack(ACK_FORMAT, raw)


def _make_request(filename, receiver_udp_port):
    name = filename.encode("utf-8")
    return struct.pack(REQUEST_HEADER_FORMAT, REQUEST_MAGIC, len(name), receiver_udp_port) + name


def _parse_request(raw):
    if len(raw) < REQUEST_HEADER_SIZE:
        return None
    magic, name_len, receiver_port = struct.unpack(
        REQUEST_HEADER_FORMAT, raw[:REQUEST_HEADER_SIZE]
    )
    if magic != REQUEST_MAGIC or len(raw) != REQUEST_HEADER_SIZE + name_len:
        return None
    try:
        filename = raw[REQUEST_HEADER_SIZE:].decode("utf-8")
    except UnicodeDecodeError:
        return None
    return filename, receiver_port


def _human_size(size):
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f} KB"
    return f"{size / (1024 * 1024):.2f} MB"


def udp_send_file(dest_ip, udp_dest_port, filepath, window_size=4,
                  timeout=2, packet_size=1024, transfer_id=None):
    if not os.path.exists(filepath):
        print(f"[ERROR] File not found: {filepath}")
        return False

    packets = file_utils.split_file_into_packets(filepath, packet_size)
    total_packets = len(packets)
    if total_packets == 0:
        packets = [b""]
        total_packets = 1

    if transfer_id is None:
        transfer_id = time.time_ns() & 0xFFFFFFFFFFFFFFFF

    print("\n" + "=" * 52)
    print("        UDP SELECTIVE REPEAT – SENDER")
    print("=" * 52)
    print(f"  File          : {os.path.basename(filepath)}")
    print(f"  Size          : {_human_size(os.path.getsize(filepath))}")
    print(f"  Packet size   : {packet_size} bytes")
    print(f"  Total chunks  : {total_packets}")
    print(f"  Window size   : {window_size}")
    print(f"  Destination   : {dest_ip}:{udp_dest_port}")
    print(f"  Transfer ID   : {transfer_id}")
    print("=" * 52)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(0.2)
    acked = [False] * total_packets
    send_time = [None] * total_packets
    next_to_send = 0
    base = 0
    retransmissions = 0

    try:
        while base < total_packets:
            # Fill the current sliding window.
            while next_to_send < total_packets and next_to_send < base + window_size:
                if not acked[next_to_send]:
                    packet = _make_packet(transfer_id, next_to_send, total_packets, packets[next_to_send])
                    sock.sendto(packet, (dest_ip, udp_dest_port))
                    send_time[next_to_send] = time.time()
                    print(f"  [SEND]  Chunk {next_to_send:>4}/{total_packets - 1}")
                next_to_send += 1

            # Collect ACKs and slide the window.
            try:
                raw_ack, _ = sock.recvfrom(64)
                parsed = _parse_ack(raw_ack)
                if parsed:
                    ack_transfer, ack_seq, ack_type = parsed
                    if ack_transfer == transfer_id and ack_type == ACK_TYPE_REGULAR:
                        if 0 <= ack_seq < total_packets:
                            acked[ack_seq] = True
                            send_time[ack_seq] = None
                            while base < total_packets and acked[base]:
                                base += 1
                            file_utils.print_progress(min(base * packet_size, os.path.getsize(filepath)), os.path.getsize(filepath), "  ACK progress")
            except socket.timeout:
                pass

            # Selective repeat: retransmit only expired unACKed chunks.
            now = time.time()
            for seq in range(base, min(base + window_size, total_packets)):
                if not acked[seq] and send_time[seq] is not None and now - send_time[seq] >= timeout:
                    packet = _make_packet(transfer_id, seq, total_packets, packets[seq])
                    sock.sendto(packet, (dest_ip, udp_dest_port))
                    send_time[seq] = time.time()
                    retransmissions += 1
                    print(f"\n  [REXMT] Chunk {seq} timeout -> retransmitted")

        done = _make_ack(transfer_id, 0, ACK_TYPE_DONE)
        for _ in range(3):
            sock.sendto(done, (dest_ip, udp_dest_port))
            time.sleep(0.03)

        print(f"\n[UDP] Transfer complete: {total_packets} chunks acknowledged.")
        print(f"[UDP] Retransmissions: {retransmissions}")
        return True
    except Exception as exc:
        print(f"[ERROR] UDP send failed: {exc}")
        return False
    finally:
        sock.close()


def udp_send_file_with_name(dest_ip, udp_dest_port, filepath,
                            window_size=4, timeout=2, packet_size=1024,
                            transfer_id=None):
    """Announce the filename, then run Selective Repeat."""
    if not os.path.exists(filepath):
        print(f"[ERROR] File not found: {filepath}")
        return False
    if transfer_id is None:
        transfer_id = time.time_ns() & 0xFFFFFFFFFFFFFFFF

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(1.0)
    filename = os.path.basename(filepath).encode("utf-8")
    announcement = struct.pack(HEADER_FORMAT, transfer_id, 0, 0, 0) + filename

    try:
        acknowledged = False
        for attempt in range(10):
            sock.sendto(announcement, (dest_ip, udp_dest_port))
            try:
                raw_ack, _ = sock.recvfrom(64)
                parsed = _parse_ack(raw_ack)
                if parsed and parsed[0] == transfer_id and parsed[1] == 0:
                    acknowledged = True
                    break
            except socket.timeout:
                print(f"[UDP] Filename ACK timeout ({attempt + 1}/10)")
        if not acknowledged:
            print("[ERROR] Receiver did not acknowledge the filename announcement.")
            return False
    finally:
        sock.close()

    return udp_send_file(dest_ip, udp_dest_port, filepath, window_size,
                         timeout, packet_size, transfer_id)


def udp_request_file(source_ip, source_udp_port, filename, my_udp_port):
    """Ask a peer to send a file to this peer's UDP listener."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(5)
    try:
        request = _make_request(filename, my_udp_port)
        sock.sendto(request, (source_ip, source_udp_port))
        reply, _ = sock.recvfrom(128)
        if reply == REQUEST_ACK:
            print(f"[UDP DOWNLOAD] Request accepted for '{filename}'.")
            return True
        print("[UDP DOWNLOAD] Peer rejected the request.")
        return False
    except Exception as exc:
        print(f"[UDP DOWNLOAD] Request failed: {exc}")
        return False
    finally:
        sock.close()


def _serve_udp_request(request_sock, addr, shared_folder, filename, receiver_port,
                       window_size, timeout, packet_size):
    """Serve one UDP download request in its own thread."""
    path = os.path.join(shared_folder, filename)
    if not os.path.isfile(path):
        print(f"[UDP REQUEST] File not found: {filename}")
        return
    request_sock.sendto(REQUEST_ACK, addr)
    # The requester may be on another computer, so use its IP and supplied port.
    udp_send_file_with_name(addr[0], receiver_port, path,
                            window_size, timeout, packet_size)


def udp_receive_file(udp_port, shared_folder="received", loss_rate=0.10,
                     window_size=4, timeout=2, packet_size=1024):
    """Permanent UDP dispatcher; multiple transfers can run concurrently."""
    os.makedirs(shared_folder, exist_ok=True)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", udp_port))
    print(f"\n[UDP] Receiver listening on port {udp_port} (concurrent mode)")

    # transfer_id -> state
    transfers = {}
    lock = threading.Lock()

    try:
        while True:
            raw, addr = sock.recvfrom(65535)

            # File request from another peer.
            request = _parse_request(raw)
            if request:
                filename, receiver_port = request
                print(f"\n[UDP REQUEST] {addr[0]} wants '{filename}' -> port {receiver_port}")
                threading.Thread(
                    target=_serve_udp_request,
                    args=(sock, addr, config.SHARED_FOLDER, filename, receiver_port,
                          window_size, timeout, packet_size),
                    daemon=True,
                ).start()
                continue

            if len(raw) == ACK_SIZE:
                # DONE packet: sender tells us it finished. The transfer is already
                # considered complete when all chunks have arrived, so ignore it.
                continue

            parsed = _parse_packet(raw)
            if not parsed:
                continue
            transfer_id, seq_no, total_packets, is_last, data = parsed

            with lock:
                state = transfers.get(transfer_id)
                if total_packets == 0:
                    # Filename announcement.
                    filename = data.decode("utf-8", errors="replace")
                    transfers[transfer_id] = {
                        "filename": filename,
                        "total": None,
                        "packets": {},
                        "sender": addr,
                        "lost": 0,
                    }
                    sock.sendto(_make_ack(transfer_id, 0), addr)
                    print(f"\n[UDP] Incoming file: {filename} from {addr[0]}")
                    continue

                if state is None:
                    # Data arrived before the announcement; create a state anyway.
                    state = {
                        "filename": None,
                        "total": total_packets,
                        "packets": {},
                        "sender": addr,
                        "lost": 0,
                    }
                    transfers[transfer_id] = state
                elif state["total"] is None:
                    state["total"] = total_packets

                # Simulate packet loss at the receiver.
                if random.random() < loss_rate:
                    state["lost"] += 1
                    print(f"  [LOSS]  Chunk {seq_no} lost (transfer {transfer_id})")
                    continue

                # Duplicate packet: ACK it again.
                if seq_no in state["packets"]:
                    sock.sendto(_make_ack(transfer_id, seq_no), addr)
                    continue

                state["packets"][seq_no] = data
                sock.sendto(_make_ack(transfer_id, seq_no), addr)

                total = state["total"]
                if total and len(state["packets"]) == total:
                    filename = state["filename"] or f"received_{transfer_id}.bin"
                    output = os.path.join(shared_folder, "received_" + os.path.basename(filename))
                    if os.path.exists(output):
                        stem, ext = os.path.splitext(output)
                        output = f"{stem}_{transfer_id}{ext}"
                    packets = state["packets"]
                    lost = state["lost"]
                    del transfers[transfer_id]

                    # Reassemble outside the lock.
                    print(f"\n[UDP] Transfer complete: {filename} ({total} chunks)")
                    if file_utils.reassemble_file(packets, output):
                        print(f"[UDP] Saved to: {output}")
                        print(f"[UDP] Simulated losses: {lost}")
    except Exception as exc:
        print(f"[UDP] Receiver error: {exc}")
    finally:
        sock.close()
