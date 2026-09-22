"""
Main Entry Point for Decentralized Multi-Threaded P2P File-Sharing System.

Launches a peer node with custom Selective Repeat UDP transport, decentralized
discovery, multi-peer chunk downloads, and an interactive terminal dashboard.
"""

import os
import sys
import time
import argparse
import shutil

from peer.peer import PeerNode
from ui.dashboard import Dashboard
from ui.logger import log_event, Colors
from benchmark import run_all_benchmarks
from config import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    DEFAULT_WINDOW_SIZE,
    DEFAULT_TIMEOUT,
    DEFAULT_LOSS_PROB,
    SHARED_DIR,
    DOWNLOADS_DIR,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Decentralized Multi-Threaded P2P File-Sharing System (Selective Repeat over UDP)"
    )
    parser.add_argument("--peer-id", type=str, default="", help="Unique identifier for this peer (e.g. PEER_A)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"UDP port to bind (default: {DEFAULT_PORT})")
    parser.add_argument("--host", type=str, default=DEFAULT_HOST, help=f"Host IP to bind (default: {DEFAULT_HOST})")
    parser.add_argument("--shared-dir", type=str, default="", help="Directory containing locally shared files")
    parser.add_argument("--downloads-dir", type=str, default="", help="Directory for saved downloads")
    parser.add_argument("--window-size", type=int, default=DEFAULT_WINDOW_SIZE, help=f"Sliding window size (default: {DEFAULT_WINDOW_SIZE})")
    parser.add_argument("--loss", type=float, default=DEFAULT_LOSS_PROB, help="Simulated packet loss probability (0.0 to 0.5)")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help=f"Retransmission timeout in seconds (default: {DEFAULT_TIMEOUT})")
    parser.add_argument("--headless", action="store_true", help="Run in non-interactive headless daemon mode")
    return parser.parse_args()


def interactive_cli(peer: PeerNode):
    """Run interactive terminal menu loop for peer node."""
    while True:
        try:
            Dashboard.print_banner(
                peer_id=peer.peer_id,
                host=peer.host,
                port=peer.port,
                window_size=peer.window_size,
                loss_prob=peer.loss_prob,
            )

            print(f"\n{Colors.BOLD}Select an Option:{Colors.RESET}")
            print(f"  {Colors.GREEN}[1]{Colors.RESET} List Active Peers & Network Files")
            print(f"  {Colors.GREEN}[2]{Colors.RESET} Download File from Network (Multi-Peer)")
            print(f"  {Colors.GREEN}[3]{Colors.RESET} Share a New File")
            print(f"  {Colors.GREEN}[4]{Colors.RESET} Configure Simulated Packet Loss %")
            print(f"  {Colors.GREEN}[5]{Colors.RESET} Configure Sliding Window Size")
            print(f"  {Colors.GREEN}[6]{Colors.RESET} View Transfer & Protocol Statistics")
            print(f"  {Colors.GREEN}[7]{Colors.RESET} Run Selective Repeat Benchmarks")
            print(f"  {Colors.GREEN}[8]{Colors.RESET} Exit Peer\n")

            choice = input(f"{Colors.CYAN}Enter selection [1-8] > {Colors.RESET}").strip()

            if choice == "1":
                active_peers = peer.discovery.get_active_peers()
                network_files = peer.discovery.get_all_network_files()
                local_files = peer.chunk_manager.list_shared_files()
                Dashboard.print_network_status(active_peers, network_files, local_files)
                input(f"\n{Colors.DIM}Press Enter to continue...{Colors.RESET}")

            elif choice == "2":
                active_peers = peer.discovery.get_active_peers()
                network_files = peer.discovery.get_all_network_files()
                local_files = peer.chunk_manager.list_shared_files()
                Dashboard.print_network_status(active_peers, network_files, local_files)

                if not network_files:
                    print(f"\n{Colors.YELLOW}[!] No remote files detected on network.{Colors.RESET}")
                    input(f"{Colors.DIM}Press Enter to return...{Colors.RESET}")
                    continue

                file_to_download = input(f"\n{Colors.BOLD}Enter file name to download > {Colors.RESET}").strip()
                if not file_to_download:
                    continue

                print(f"\n{Colors.CYAN}--> Initiating multi-peer download for '{file_to_download}'...{Colors.RESET}")
                success, msg, final_path = peer.download_file(file_to_download)

                if success:
                    print(f"\n{Colors.GREEN}{Colors.BOLD}[SUCCESS] {msg}{Colors.RESET}")
                    print(f"  Saved to: {final_path}")
                    # Print statistics
                    summary = peer.stats.get_summary()
                    file_size = os.path.getsize(final_path) if os.path.exists(final_path) else 0
                    Dashboard.print_stats_summary(summary, file_name=file_to_download, file_size_bytes=file_size)
                else:
                    print(f"\n{Colors.RED}{Colors.BOLD}[FAILED] {msg}{Colors.RESET}")

                input(f"\n{Colors.DIM}Press Enter to continue...{Colors.RESET}")

            elif choice == "3":
                source_path = input(f"{Colors.BOLD}Enter absolute path of file to share > {Colors.RESET}").strip('"\' ')
                if os.path.isfile(source_path):
                    target_name = os.path.basename(source_path)
                    dest_file = os.path.join(peer.chunk_manager.shared_dir, target_name)
                    shutil.copyfile(source_path, dest_file)
                    peer.chunk_manager.refresh_shared_files()
                    print(f"{Colors.GREEN}[+] Successfully shared '{target_name}' across the P2P network!{Colors.RESET}")
                else:
                    print(f"{Colors.RED}[!] Invalid file path.{Colors.RESET}")
                input(f"\n{Colors.DIM}Press Enter to continue...{Colors.RESET}")

            elif choice == "4":
                try:
                    loss_pct = float(input(f"{Colors.BOLD}Enter packet loss percentage (0 - 50%) > {Colors.RESET}").strip())
                    peer.set_loss_probability(loss_pct / 100.0)
                    print(f"{Colors.GREEN}[+] Packet loss set to {loss_pct:.1f}%{Colors.RESET}")
                except ValueError:
                    print(f"{Colors.RED}[!] Invalid number entered.{Colors.RESET}")
                time.sleep(1.0)

            elif choice == "5":
                try:
                    win = int(input(f"{Colors.BOLD}Enter sliding window size (e.g. 4, 8, 16) > {Colors.RESET}").strip())
                    peer.set_window_size(win)
                    print(f"{Colors.GREEN}[+] Window size set to {win}{Colors.RESET}")
                except ValueError:
                    print(f"{Colors.RED}[!] Invalid number entered.{Colors.RESET}")
                time.sleep(1.0)

            elif choice == "6":
                summary = peer.stats.get_summary()
                Dashboard.print_stats_summary(summary)
                input(f"{Colors.DIM}Press Enter to return...{Colors.RESET}")

            elif choice == "7":
                print(f"\n{Colors.CYAN}--> Starting Automated Selective Repeat Benchmarks...{Colors.RESET}")
                run_all_benchmarks(payload_size_kb=200)
                input(f"\n{Colors.DIM}Press Enter to return to menu...{Colors.RESET}")

            elif choice == "8":
                print(f"\n{Colors.YELLOW}Shutting down peer node...{Colors.RESET}")
                break

        except KeyboardInterrupt:
            print(f"\n{Colors.YELLOW}Shutting down peer node...{Colors.RESET}")
            break
        except Exception as e:
            print(f"\n{Colors.RED}[ERROR] {e}{Colors.RESET}")
            time.sleep(2.0)


def main():
    args = parse_args()

    peer_id = args.peer_id or f"PEER_{args.port}"
    shared_dir = args.shared_dir or os.path.join(SHARED_DIR, peer_id.lower())
    downloads_dir = args.downloads_dir or os.path.join(DOWNLOADS_DIR, peer_id.lower())

    os.makedirs(shared_dir, exist_ok=True)
    os.makedirs(downloads_dir, exist_ok=True)

    peer = PeerNode(
        peer_id=peer_id,
        host=args.host,
        port=args.port,
        shared_dir=shared_dir,
        downloads_dir=downloads_dir,
        window_size=args.window_size,
        timeout=args.timeout,
        loss_prob=args.loss,
        log_callback=log_event,
    )

    peer.start()

    if args.headless:
        log_event(f"[PEER] Running in headless mode on port {args.port}. Press Ctrl+C to stop.")
        try:
            while True:
                time.sleep(1.0)
        except KeyboardInterrupt:
            pass
        finally:
            peer.stop()
    else:
        try:
            interactive_cli(peer)
        finally:
            peer.stop()


if __name__ == "__main__":
    main()
