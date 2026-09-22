"""
Interactive Dashboard and Terminal Interface.

Provides styled CLI dashboards, live transfer progress visualization,
network topology display, and statistics presentation.
"""

import os
import sys
import time
from typing import Dict, List, Any

from .logger import Colors
from config import get_local_ip


class Dashboard:
    """
    Renders terminal dashboards, progress bars, and formatted status reports.
    """

    @staticmethod
    def clear_screen() -> None:
        """Clear console screen across Windows and Unix."""
        os.system("cls" if os.name == "nt" else "clear")

    @staticmethod
    def print_banner(peer_id: str, host: str, port: int, window_size: int, loss_prob: float) -> None:
        """Display stylish top banner with peer identity and configuration."""
        local_ip = get_local_ip()
        endpoint_display = f"{local_ip}:{port} (listening on {host}:{port})" if host in ("0.0.0.0", "") else f"{host}:{port}"
        print(f"\n{Colors.CYAN}==============================================================={Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.WHITE}        DECENTRALIZED P2P FILE-SHARING SYSTEM (SR-UDP)        {Colors.RESET}")
        print(f"{Colors.CYAN}==============================================================={Colors.RESET}")
        print(f"  {Colors.BOLD}Peer ID    :{Colors.RESET} {Colors.GREEN}{peer_id}{Colors.RESET}")
        print(f"  {Colors.BOLD}Endpoint   :{Colors.RESET} {endpoint_display}")
        print(f"  {Colors.BOLD}Transport  :{Colors.RESET} Custom Selective Repeat Sliding Window over UDP")
        print(f"  {Colors.BOLD}Window Size:{Colors.RESET} {window_size} packets | {Colors.BOLD}Packet Loss:{Colors.RESET} {loss_prob * 100:.1f}%")
        print(f"{Colors.CYAN}---------------------------------------------------------------{Colors.RESET}")

    @staticmethod
    def print_network_status(active_peers: Dict[str, Dict[str, Any]], network_files: Dict[str, List[str]], local_files: List[str]) -> None:
        """Display connected peers and available files."""
        print(f"\n{Colors.BOLD}{Colors.YELLOW}[ CONNECTED PEERS ({len(active_peers)}) ]{Colors.RESET}")
        if not active_peers:
            print("  (No remote peers detected yet. Searching local network...)")
        else:
            print(f"  {'PEER ID':<15} {'ENDPOINT':<22} {'SHARED FILES'}")
            print(f"  {'-'*15} {'-'*22} {'-'*25}")
            for pid, info in active_peers.items():
                ep = f"{info['ip']}:{info['port']}"
                files_str = ", ".join(info.get("shared_files", [])) or "(none)"
                print(f"  {pid:<15} {ep:<22} {files_str}")

        print(f"\n{Colors.BOLD}{Colors.YELLOW}[ AVAILABLE NETWORK FILES ]{Colors.RESET}")
        if not network_files and not local_files:
            print("  (No files shared on network)")
        else:
            all_files = set(network_files.keys()).union(set(local_files))
            print(f"  {'FILE NAME':<25} {'HOLDERS / SEEDERS'}")
            print(f"  {'-'*25} {'-'*35}")
            for fname in sorted(all_files):
                holders = []
                if fname in local_files:
                    holders.append(f"{Colors.GREEN}Local{Colors.RESET}")
                remote_holders = network_files.get(fname, [])
                holders.extend(remote_holders)
                print(f"  {fname:<25} {', '.join(holders)}")

        print(f"{Colors.CYAN}---------------------------------------------------------------{Colors.RESET}")

    @staticmethod
    def print_progress_bar(done: int, total: int, speed_kbps: float, file_name: str) -> None:
        """Render a graphical progress bar for active chunk downloads."""
        pct = (done / total * 100.0) if total > 0 else 100.0
        bar_len = 30
        filled = int(bar_len * done / total) if total > 0 else bar_len
        bar = "#" * filled + "-" * (bar_len - filled)
        sys.stdout.write(
            f"\r{Colors.BOLD}Downloading {file_name}:{Colors.RESET} [{bar}] {pct:5.1f}% "
            f"({done}/{total} chunks) @ {speed_kbps:6.1f} KB/s  "
        )
        sys.stdout.flush()
        if done >= total:
            sys.stdout.write("\n")
            sys.stdout.flush()

    @staticmethod
    def print_stats_summary(stats_dict: Dict[str, Any], file_name: str = "", file_size_bytes: int = 0) -> None:
        """Display formatted post-transfer statistics table."""
        print(f"\n{Colors.GREEN}==============================================================={Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.WHITE}                   TRANSFER SUMMARY & METRICS                  {Colors.RESET}")
        print(f"{Colors.GREEN}==============================================================={Colors.RESET}")
        if file_name:
            print(f"  {Colors.BOLD}File Transferred     :{Colors.RESET} {file_name}")
        if file_size_bytes > 0:
            print(f"  {Colors.BOLD}File Size            :{Colors.RESET} {file_size_bytes / 1024:.2f} KB ({file_size_bytes} bytes)")

        print(f"  {Colors.BOLD}Transfer Duration    :{Colors.RESET} {stats_dict['elapsed_seconds']:.2f} seconds")
        print(f"  {Colors.BOLD}Effective Throughput :{Colors.RESET} {stats_dict['throughput_kb_s']:.2f} KB/s ({stats_dict['throughput_mb_s']:.3f} MB/s)")
        print(f"  {Colors.CYAN}-------------------------------------------------------------{Colors.RESET}")
        print(f"  {Colors.BOLD}Data Packets Sent    :{Colors.RESET} {stats_dict['packets_sent']}")
        print(f"  {Colors.BOLD}Packets Received     :{Colors.RESET} {stats_dict['packets_received']}")
        print(f"  {Colors.BOLD}Simulated Losses     :{Colors.RESET} {Colors.RED}{stats_dict['packets_lost']}{Colors.RESET} ({stats_dict['loss_rate_pct']:.1f}%)")
        print(f"  {Colors.BOLD}Selective Retransmits:{Colors.RESET} {Colors.MAGENTA}{stats_dict['packets_retransmitted']}{Colors.RESET} ({stats_dict['retransmit_rate_pct']:.1f}%)")
        print(f"  {Colors.BOLD}Out-of-Order Buffered:{Colors.RESET} {stats_dict['packets_buffered']}")
        print(f"  {Colors.BOLD}Duplicate Packets    :{Colors.RESET} {stats_dict['duplicate_packets']}")
        print(f"  {Colors.BOLD}ACKs Sent / Received :{Colors.RESET} {stats_dict['acks_sent']} / {stats_dict['acks_received']}")
        print(f"{Colors.GREEN}==============================================================={Colors.RESET}\n")
