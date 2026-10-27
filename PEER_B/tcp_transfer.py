# =============================================================================
# tcp_transfer.py – TCP File Transfer (Sender and Receiver)
# =============================================================================

import socket
import os
import struct
import threading
import json

import file_utils

# ---------------------------------------------------------------------------
# SPECIAL COMMAND VALUES
# ---------------------------------------------------------------------------

LIST_MAGIC = 0xFFFFFFFF
DOWNLOAD_MAGIC = 0xFFFFFFFE


# =============================================================================
# SEND FILE OVER AN EXISTING TCP CONNECTION
# =============================================================================

def _send_file_over_connection(conn, filepath):
    """Send one complete file over an already-connected TCP socket."""

    if not os.path.isfile(filepath):
        return False

    filename = os.path.basename(filepath)
    file_size = os.path.getsize(filepath)
    filename_bytes = filename.encode("utf-8")

    # Send filename length
    conn.sendall(struct.pack("!I", len(filename_bytes)))

    # Send filename
    conn.sendall(filename_bytes)

    # Send file size
    conn.sendall(struct.pack("!Q", file_size))

    from config import PACKET_SIZE

    sent = 0

    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(PACKET_SIZE)

            if not chunk:
                break

            conn.sendall(chunk)
            sent += len(chunk)

            file_utils.print_progress(
                sent,
                file_size,
                "  Sending"
            )

    print(f"\n[TCP] Sent '{filename}' ({sent} bytes)")

    return True


# =============================================================================
# TCP SEND FILE
# =============================================================================

def tcp_send_file(dest_ip, dest_port, filepath, shared_folder="shared"):
    """Send a file to another peer using TCP."""

    if not os.path.exists(filepath):
        print(f"[ERROR] File not found: {filepath}")
        return False

    sock = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    try:
        sock.connect((dest_ip, dest_port))

        print(
            f"\n[TCP] Connected to "
            f"{dest_ip}:{dest_port}"
        )

        print(
            f"[TCP] Sending "
            f"'{os.path.basename(filepath)}' "
            f"({os.path.getsize(filepath)} bytes)"
        )

        return _send_file_over_connection(
            sock,
            filepath
        )

    except Exception as exc:

        print(
            f"[ERROR] TCP send failed: {exc}"
        )

        return False

    finally:
        sock.close()


# =============================================================================
# TCP DOWNLOAD FILE
# =============================================================================

def tcp_request_file(
    source_ip,
    source_port,
    filename,
    received_folder="received"
):
    """Request a file from another peer using TCP."""

    sock = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    sock.settimeout(30)

    try:

        sock.connect(
            (source_ip, source_port)
        )

        name = filename.encode("utf-8")

        # Tell receiver that we want to DOWNLOAD a file
        sock.sendall(
            struct.pack(
                "!I",
                DOWNLOAD_MAGIC
            )
        )

        # Send requested filename length
        sock.sendall(
            struct.pack(
                "!I",
                len(name)
            )
        )

        # Send requested filename
        sock.sendall(name)

        # Receive filename length
        filename_len = struct.unpack(
            "!I",
            _recv_exact(sock, 4)
        )[0]

        # Receive filename
        received_name = _recv_exact(
            sock,
            filename_len
        ).decode("utf-8")

        # Receive file size
        file_size = struct.unpack(
            "!Q",
            _recv_exact(sock, 8)
        )[0]

        os.makedirs(
            received_folder,
            exist_ok=True
        )

        output = os.path.join(
            received_folder,
            "received_" +
            os.path.basename(received_name)
        )

        total = 0

        with open(output, "wb") as f:

            while total < file_size:

                piece = sock.recv(
                    min(
                        65536,
                        file_size - total
                    )
                )

                if not piece:
                    raise ConnectionError(
                        "Source closed connection early"
                    )

                f.write(piece)

                total += len(piece)

                file_utils.print_progress(
                    total,
                    file_size,
                    "  Receiving"
                )

        print(
            f"\n[TCP DOWNLOAD] Saved: {output}"
        )

        print(
            f"[TCP DOWNLOAD] SHA-256: "
            f"{file_utils.sha256_hash(output)}"
        )

        return True

    except Exception as exc:

        print(
            f"[TCP DOWNLOAD] Failed for "
            f"'{filename}': {exc}"
        )

        return False

    finally:
        sock.close()


# =============================================================================
# TCP RECEIVER
# =============================================================================

def tcp_receive_file(
    tcp_port,
    shared_folder="received",
    serve_folder="shared"
):
    """
    TCP server that accepts incoming connections.

    shared_folder:
        Where received files are stored.

    serve_folder:
        Folder containing files that this peer can provide
        to other peers.
    """

    # Create folders if they don't exist
    os.makedirs(
        shared_folder,
        exist_ok=True
    )

    os.makedirs(
        serve_folder,
        exist_ok=True
    )

    # Create TCP socket
    server_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    # Allow immediate reuse of port
    server_socket.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1
    )

    try:

        # Bind server to TCP port
        server_socket.bind(
            ("", tcp_port)
        )

        # Start listening
        server_socket.listen(10)

        print(
            f"\n[TCP] Receiver is listening "
            f"on port {tcp_port}..."
        )

        print(
            "[TCP] Waiting for connections..."
        )

        while True:

            conn, sender_address = (
                server_socket.accept()
            )

            sender_ip = sender_address[0]
            sender_port = sender_address[1]

            print(
                f"\n[TCP] Incoming connection "
                f"from {sender_ip}:{sender_port}"
            )

            # Handle every connection in its own thread
            handler_thread = threading.Thread(
                target=_handle_tcp_receive,
                args=(
                    conn,
                    shared_folder,
                    serve_folder
                ),
                daemon=True
            )

            handler_thread.start()

    except Exception as e:

        print(
            f"[ERROR] TCP listener error: {e}"
        )

    finally:

        server_socket.close()


# =============================================================================
# HANDLE ONE TCP CONNECTION
# =============================================================================

def _handle_tcp_receive(
    conn,
    shared_folder,
    serve_folder="shared"
):
    """
    Handle one incoming TCP connection.

    The connection can be:

    1. Normal file upload
    2. File-list request
    3. File download request
    """

    try:

        # ---------------------------------------------------------------
        # Receive first 4 bytes
        # ---------------------------------------------------------------

        filename_len_bytes = _recv_exact(
            conn,
            4
        )

        filename_len = struct.unpack(
            "!I",
            filename_len_bytes
        )[0]

        # ---------------------------------------------------------------
        # FILE LIST REQUEST
        # ---------------------------------------------------------------

        if filename_len == LIST_MAGIC:

            _serve_file_list(
                conn,
                serve_folder
            )

            return

        # ---------------------------------------------------------------
        # FILE DOWNLOAD REQUEST
        # ---------------------------------------------------------------

        if filename_len == DOWNLOAD_MAGIC:

            requested_len = struct.unpack(
                "!I",
                _recv_exact(conn, 4)
            )[0]

            requested_name = _recv_exact(
                conn,
                requested_len
            ).decode("utf-8")

            # IMPORTANT:
            # The requested file comes from serve_folder
            requested_path = os.path.join(
                serve_folder,
                requested_name
            )

            if not os.path.isfile(
                requested_path
            ):

                print(
                    f"[TCP REQUEST] "
                    f"File not found: "
                    f"{requested_name}"
                )

                return

            print(
                f"\n[TCP REQUEST] Sending "
                f"requested file: "
                f"{requested_name}"
            )

            _send_file_over_connection(
                conn,
                requested_path
            )

            return

        # ---------------------------------------------------------------
        # NORMAL FILE UPLOAD
        # ---------------------------------------------------------------

        filename_bytes = _recv_exact(
            conn,
            filename_len
        )

        filename = filename_bytes.decode(
            "utf-8"
        )

        file_size_bytes = _recv_exact(
            conn,
            8
        )

        file_size = struct.unpack(
            "!Q",
            file_size_bytes
        )[0]

        print(
            f"\n[TCP] Receiving file: "
            f"'{filename}' "
            f"({file_size} bytes)"
        )

        # Save received file
        output_path = os.path.join(
            shared_folder,
            "received_" + filename
        )

        bytes_received = 0

        with open(
            output_path,
            "wb"
        ) as f:

            while bytes_received < file_size:

                remaining = (
                    file_size -
                    bytes_received
                )

                chunk = conn.recv(
                    min(
                        1024,
                        remaining
                    )
                )

                if not chunk:

                    print(
                        "\n[ERROR] Connection "
                        "closed before file "
                        "was fully received!"
                    )

                    return

                f.write(chunk)

                bytes_received += len(chunk)

                file_utils.print_progress(
                    bytes_received,
                    file_size,
                    "  Receiving"
                )

        print(
            f"\n[TCP] File received!"
        )

        print(
            f"[TCP] Saved to: "
            f"{output_path}"
        )

        print(
            f"[TCP] Total bytes received: "
            f"{bytes_received}"
        )

        received_hash = (
            file_utils.sha256_hash(
                output_path
            )
        )

        print(
            f"\n[TCP] SHA-256: "
            f"{received_hash}"
        )

    except Exception as e:

        print(
            f"\n[ERROR] TCP receive error: {e}"
        )

    finally:

        conn.close()


# =============================================================================
# RECEIVE EXACT NUMBER OF BYTES
# =============================================================================

def _recv_exact(
    sock,
    num_bytes
):
    """
    TCP may return fewer bytes than requested.

    This function keeps receiving until exactly
    num_bytes have been received.
    """

    data = b""

    while len(data) < num_bytes:

        chunk = sock.recv(
            num_bytes - len(data)
        )

        if not chunk:

            raise ConnectionError(
                "Connection closed unexpectedly."
            )

        data += chunk

    return data


# =============================================================================
# SERVE FILE LIST
# =============================================================================

def _serve_file_list(
    conn,
    serve_folder
):
    """
    Send the list of files in the peer's
    shared folder.

    IMPORTANT FIX:
    This function uses serve_folder.
    """

    files = []

    # Make sure the folder exists
    os.makedirs(
        serve_folder,
        exist_ok=True
    )

    # Read files from shared folder
    for fname in sorted(
        os.listdir(serve_folder)
    ):

        fpath = os.path.join(
            serve_folder,
            fname
        )

        if not os.path.isfile(
            fpath
        ):
            continue

        size = os.path.getsize(
            fpath
        )

        # Convert size to readable format
        if size < 1024:

            size_str = (
                f"{size} B"
            )

        elif size < 1024 * 1024:

            size_str = (
                f"{size // 1024} KB"
            )

        else:

            size_str = (
                f"{size / (1024 * 1024):.1f} MB"
            )

        files.append({
            "name": fname,
            "size": size,
            "size_str": size_str
        })

    # Convert list to JSON
    response_bytes = json.dumps(
        files
    ).encode("utf-8")

    # Send response length first
    conn.sendall(
        struct.pack(
            "!I",
            len(response_bytes)
        )
    )

    # Send JSON data
    conn.sendall(
        response_bytes
    )


# =============================================================================
# QUERY ANOTHER PEER'S FILE LIST
# =============================================================================

def tcp_query_peer_files(
    peer_ip,
    peer_tcp_port,
    timeout=5
):
    """
    Ask another peer for the files
    available in its shared folder.
    """

    sock = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    sock.settimeout(timeout)

    try:

        sock.connect(
            (
                peer_ip,
                peer_tcp_port
            )
        )

        # Tell remote peer:
        # "I want your file list"
        sock.sendall(
            struct.pack(
                "!I",
                LIST_MAGIC
            )
        )

        # Receive JSON length
        length_bytes = _recv_exact(
            sock,
            4
        )

        response_len = struct.unpack(
            "!I",
            length_bytes
        )[0]

        # Receive JSON
        json_bytes = _recv_exact(
            sock,
            response_len
        )

        files = json.loads(
            json_bytes.decode("utf-8")
        )

        return files

    except ConnectionRefusedError:

        return None

    except socket.timeout:

        return None

    except Exception:

        return None

    finally:

        sock.close()