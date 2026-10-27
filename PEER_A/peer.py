"""Main CLI for the decentralized 3-peer file-sharing project.

Each peer can both send and download files.  TCP and UDP are separate menu
choices.  UDP uses Selective Repeat internally; users never choose or send
individual chunks.  TCP/UDP listeners run in background threads, so a peer can
receive multiple transfers while its menu remains usable.
"""
import os
import threading

import config
import file_utils
import tcp_transfer
import udp_selective_repeat as udp_sr


tcp_listener_started = False
udp_listener_started = False
my_tcp_port = config.TCP_PORT
my_udp_port = config.UDP_PORT


def print_header(title):
    print("\n" + "=" * 56)
    print("  " + title.center(52))
    print("=" * 56)


def pause():
    input("\n  Press Enter to return to the menu...")


def start_peer():
    """Start permanent TCP and UDP receiver listeners."""
    global tcp_listener_started, udp_listener_started
    print_header("START PEER / ENABLE RECEIVING")

    os.makedirs(config.SHARED_FOLDER, exist_ok=True)
    os.makedirs(config.RECEIVED_FOLDER, exist_ok=True)

    if not tcp_listener_started:
        threading.Thread(
            target=tcp_transfer.tcp_receive_file,
            args=(my_tcp_port, config.RECEIVED_FOLDER, config.SHARED_FOLDER),
            daemon=True,
        ).start()
        tcp_listener_started = True
        print(f"[OK] TCP listener running on port {my_tcp_port}")
    else:
        print("[--] TCP listener already running")

    if not udp_listener_started:
        threading.Thread(
            target=udp_sr.udp_receive_file,
            args=(my_udp_port, config.RECEIVED_FOLDER, config.LOSS_RATE,
                  config.WINDOW_SIZE, config.TIMEOUT, config.PACKET_SIZE),
            daemon=True,
        ).start()
        udp_listener_started = True
        print(f"[OK] UDP listener running on port {my_udp_port}")
    else:
        print("[--] UDP listener already running")

    print(f"\nShared folder  : {config.SHARED_FOLDER}/")
    print(f"Received folder: {config.RECEIVED_FOLDER}/")
    print(f"TCP port       : {my_tcp_port}")
    print(f"UDP port       : {my_udp_port}")
    print("\nThis peer can now send AND receive files.")
    pause()


def list_my_files():
    print_header("MY SHARED FILES")
    file_utils.list_shared_files(config.SHARED_FOLDER)
    pause()


def list_files_by_peer():
    print_header("FILES AVAILABLE BY PEER")
    print("  Querying all known peers...\n")
    results = {}
    lock = threading.Lock()

    def query(peer):
        files = tcp_transfer.tcp_query_peer_files(peer["ip"], peer["tcp_port"], timeout=5)
        with lock:
            results[peer["name"]] = files

    threads = [threading.Thread(target=query, args=(p,), daemon=True) for p in config.KNOWN_PEERS]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=6)

    for i, peer in enumerate(config.KNOWN_PEERS, 1):
        files = results.get(peer["name"])
        print(f"\n{i}. {peer['name']}  {peer['ip']}")
        if files is None:
            print("   [OFFLINE / not reachable]")
        elif not files:
            print("   [No files]")
        else:
            for f in files:
                print(f"   - {f['name']} ({f['size_str']})")
    pause()


def _choose_peer(protocol):
    print("\nKnown peers:")
    for i, p in enumerate(config.KNOWN_PEERS, 1):
        port = p["tcp_port"] if protocol == "TCP" else p["udp_port"]
        print(f"  {i}. {p['name']}  {p['ip']}:{port}")
    try:
        n = int(input("  Select peer number: ").strip())
        if not 1 <= n <= len(config.KNOWN_PEERS):
            raise ValueError
        return config.KNOWN_PEERS[n - 1]
    except ValueError:
        print("  [ERROR] Invalid peer selection.")
        return None


def tcp_send_menu():
    print_header("TCP – SEND FILE")
    files = file_utils.list_shared_files(config.SHARED_FOLDER)
    if not files:
        pause(); return
    try:
        n = int(input("\nSelect file number: "))
        filename = files[n - 1]
    except (ValueError, IndexError):
        print("[ERROR] Invalid file number."); pause(); return

    peer = _choose_peer("TCP")
    if not peer:
        pause(); return
    path = os.path.join(config.SHARED_FOLDER, filename)
    # A separate thread means the menu is immediately available for another transfer.
    threading.Thread(
        target=tcp_transfer.tcp_send_file,
        args=(peer["ip"], peer["tcp_port"], path),
        daemon=True,
    ).start()
    print(f"\n[TCP] Send started in background: {filename} -> {peer['name']}")
    pause()


def tcp_download_one(peer, filename):
    return tcp_transfer.tcp_request_file(
        peer["ip"], peer["tcp_port"], filename, config.RECEIVED_FOLDER
    )


def tcp_download_menu():
    print_header("TCP – DOWNLOAD FROM PEER")
    print("  You can start multiple downloads at once.")
    try:
        count = int(input("  Number of files to download concurrently [2]: ") or "2")
        if count < 1:
            raise ValueError
    except ValueError:
        print("[ERROR] Invalid number."); pause(); return

    jobs = []
    for i in range(count):
        print(f"\n  Download {i + 1}/{count}")
        peer = _choose_peer("TCP")
        if not peer:
            continue
        filename = input("  Filename to download: ").strip()
        if filename:
            jobs.append((peer, filename))

    if not jobs:
        print("[ERROR] No download jobs selected."); pause(); return

    print(f"\n[TCP] Starting {len(jobs)} download(s) concurrently...")
    threads = []
    for peer, filename in jobs:
        t = threading.Thread(target=tcp_download_one, args=(peer, filename), daemon=True)
        threads.append(t)
        t.start()
    print("[TCP] All download threads started. They run concurrently in the background.")
    pause()


def udp_send_menu():
    print_header("UDP – SEND FILE (SELECTIVE REPEAT)")
    files = file_utils.list_shared_files(config.SHARED_FOLDER)
    if not files:
        pause(); return
    try:
        n = int(input("\nSelect file number: "))
        filename = files[n - 1]
    except (ValueError, IndexError):
        print("[ERROR] Invalid file number."); pause(); return

    peer = _choose_peer("UDP")
    if not peer:
        pause(); return
    path = os.path.join(config.SHARED_FOLDER, filename)
    size = os.path.getsize(path)
    chunks = (size + config.PACKET_SIZE - 1) // config.PACKET_SIZE
    print(f"\nFile size: {size} bytes")
    print(f"UDP chunks: {chunks} (automatic; no chunk input required)")
    print(f"Window size: {config.WINDOW_SIZE}")
    print(f"Timeout: {config.TIMEOUT} s")
    print(f"Simulated loss: {config.LOSS_RATE * 100:.0f}%")

    threading.Thread(
        target=udp_sr.udp_send_file_with_name,
        args=(peer["ip"], peer["udp_port"], path),
        kwargs={"window_size": config.WINDOW_SIZE,
                "timeout": config.TIMEOUT,
                "packet_size": config.PACKET_SIZE},
        daemon=True,
    ).start()
    print(f"\n[UDP] Send started in background: {filename} -> {peer['name']}")
    pause()


def udp_download_one(peer, filename):
    return udp_sr.udp_request_file(
        peer["ip"], peer["udp_port"], filename, my_udp_port
    )


def udp_download_menu():
    print_header("UDP – DOWNLOAD FROM PEER")
    print("  Select files from other peers. Chunks are automatic.")
    print("  Selective Repeat, sliding window, ACKs, loss and retransmission")
    print("  are handled internally by the protocol.\n")
    try:
        count = int(input("  Number of files to download concurrently [2]: ") or "2")
        if count < 1:
            raise ValueError
    except ValueError:
        print("[ERROR] Invalid number."); pause(); return

    jobs = []
    for i in range(count):
        print(f"\n  Download {i + 1}/{count}")
        peer = _choose_peer("UDP")
        if not peer:
            continue
        filename = input("  Filename to download: ").strip()
        if filename:
            jobs.append((peer, filename))

    if not jobs:
        print("[ERROR] No download jobs selected."); pause(); return

    print(f"\n[UDP] Starting {len(jobs)} download request(s) concurrently...")
    threads = []
    for peer, filename in jobs:
        t = threading.Thread(target=udp_download_one, args=(peer, filename), daemon=True)
        threads.append(t)
        t.start()
    print("[UDP] All request threads started. The files/chunks are handled automatically.")
    pause()


def verify_files_menu():
    print_header("SHA-256 FILE VERIFICATION")
    print("Enter paths or filenames. Received files are normally in received/.")
    original = input("  Original file path: ").strip()
    received = input("  Received file path: ").strip()
    if not os.path.exists(original) and os.path.exists(os.path.join(config.SHARED_FOLDER, original)):
        original = os.path.join(config.SHARED_FOLDER, original)
    if not os.path.exists(received) and os.path.exists(os.path.join(config.RECEIVED_FOLDER, received)):
        received = os.path.join(config.RECEIVED_FOLDER, received)
    file_utils.print_hash_comparison(original, received)
    pause()


def configure_this_peer():
    global my_tcp_port, my_udp_port
    print_header("PEER CONFIGURATION")
    print("Ports come from config.py. Press Enter to keep them.")
    try:
        tcp_in = input(f"  TCP port [{config.TCP_PORT}]: ").strip()
        udp_in = input(f"  UDP port [{config.UDP_PORT}]: ").strip()
        my_tcp_port = int(tcp_in) if tcp_in else config.TCP_PORT
        my_udp_port = int(udp_in) if udp_in else config.UDP_PORT
    except ValueError:
        print("[WARN] Invalid input; using config.py values.")
        my_tcp_port, my_udp_port = config.TCP_PORT, config.UDP_PORT

    os.makedirs(config.SHARED_FOLDER, exist_ok=True)
    os.makedirs(config.RECEIVED_FOLDER, exist_ok=True)
    print(f"\nPeer: {config.PEER_NAME}")
    print(f"TCP: {my_tcp_port}   UDP: {my_udp_port}")
    print(f"Packet size: {config.PACKET_SIZE} bytes")
    print(f"UDP window: {config.WINDOW_SIZE}")
    print(f"UDP timeout: {config.TIMEOUT}s")
    print(f"UDP loss: {config.LOSS_RATE * 100:.0f}%")
    pause()


def main_menu():
    while True:
        print("\n" + "=" * 56)
        print("              P2P FILE SHARING SYSTEM")
        print(f"                    {config.PEER_NAME}")
        print("=" * 56)
        print("  1. Start Peer / Enable Receiving")
        print("  2. List My Shared Files")
        print("  3. List Files Available By Peer")
        print("  4. TCP  – Send File")
        print("  5. TCP  – Download File(s) Concurrently")
        print("  6. UDP  – Send File (Selective Repeat)")
        print("  7. UDP  – Download File(s) Concurrently")
        print("  8. Verify Files (SHA-256)")
        print("  9. Exit")
        print("=" * 56)
        choice = input("  Enter choice (1-9): ").strip()
        if choice == "1": start_peer()
        elif choice == "2": list_my_files()
        elif choice == "3": list_files_by_peer()
        elif choice == "4": tcp_send_menu()
        elif choice == "5": tcp_download_menu()
        elif choice == "6": udp_send_menu()
        elif choice == "7": udp_download_menu()
        elif choice == "8": verify_files_menu()
        elif choice == "9": print("\nGoodbye!"); break
        else:
            print("[ERROR] Choose 1-9.")
            pause()


if __name__ == "__main__":
    configure_this_peer()
    main_menu()
