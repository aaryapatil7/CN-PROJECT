"""
Colorized Networking and Event Logger.

Formats protocol events ([DATA], [ACK], [LOSS], [TIMEOUT], [RETRANSMIT], etc.)
with distinct ANSI colors for academic demonstration and live debugging.
"""

import sys
import threading
from datetime import datetime


class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"

    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    WHITE = "\033[97m"

    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_BLUE = "\033[44m"


TAG_COLORS = {
    "[DISCOVERY]": Colors.CYAN,
    "[REQUEST]": Colors.BLUE,
    "[DATA]": Colors.WHITE,
    "[ACK]": Colors.GREEN,
    "[LOSS]": Colors.RED + Colors.BOLD,
    "[CORRUPT]": Colors.RED + Colors.BOLD,
    "[TIMEOUT]": Colors.YELLOW + Colors.BOLD,
    "[RETRANSMIT]": Colors.MAGENTA + Colors.BOLD,
    "[BUFFER]": Colors.YELLOW,
    "[WINDOW]": Colors.CYAN,
    "[PROGRESS]": Colors.GREEN,
    "[COMPLETE]": Colors.GREEN + Colors.BOLD,
    "[ERROR]": Colors.RED + Colors.BOLD,
    "[WARN]": Colors.YELLOW,
    "[CONFIG]": Colors.BLUE,
    "[RESUME]": Colors.MAGENTA,
    "[REASSEMBLER]": Colors.CYAN,
    "[PEER]": Colors.CYAN + Colors.BOLD,
    "[SENDER]": Colors.BLUE,
}

_log_lock = threading.Lock()
_enable_color = True


def format_log_line(message: str) -> str:
    """Format message with timestamp and tag highlighting."""
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]

    colored_msg = message
    if _enable_color:
        for tag, color in TAG_COLORS.items():
            if tag in message:
                colored_msg = message.replace(tag, f"{color}{tag}{Colors.RESET}")
                break

    return f"{Colors.DIM}[{timestamp}]{Colors.RESET} {colored_msg}"


def log_event(message: str) -> None:
    """Thread-safe stdout printer for networking events."""
    line = format_log_line(message)
    with _log_lock:
        print(line, flush=True)
