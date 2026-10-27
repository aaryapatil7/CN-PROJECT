# =============================================================================
# generate_samples.py – Creates Sample Files for the P2P Demo
# =============================================================================
# Run this ONCE before the demo to create sample PDF, image, and video files
# in the shared/ folder.
#
#   python generate_samples.py
#
# Files created:
#   shared/sample.txt        – Text file (already exists)
#   shared/sample.pdf        – Minimal valid PDF document
#   shared/sample_image.png  – Tiny PNG image (valid binary)
#   shared/sample_video.mp4  – Dummy video file (~200 KB binary data)
# =============================================================================

import os
import struct
import zlib
import config

SHARED_FOLDER = config.SHARED_FOLDER
prefix = config.PEER_NAME.replace(" ", "_")

# Make sure the shared folder exists
if not os.path.exists(SHARED_FOLDER):
    os.makedirs(SHARED_FOLDER)
    print(f"Created folder: {SHARED_FOLDER}/")


# =============================================================================
# 1. SAMPLE TEXT FILE (sample.txt)
# =============================================================================

txt_path = os.path.join(SHARED_FOLDER, f"{prefix}_notes.txt")
if not os.path.exists(txt_path):
    with open(txt_path, "w") as f:
        f.write("=" * 60 + "\n")
        f.write("  Computer Networks – P2P File Sharing Demo\n")
        f.write("  Selective Repeat Sliding Window Protocol\n")
        f.write("=" * 60 + "\n\n")
        f.write("This sample text file is used to demonstrate:\n")
        f.write("  1. TCP file transfer (built-in reliability)\n")
        f.write("  2. UDP + Selective Repeat (custom reliability)\n\n")
        f.write("After transfer, SHA-256 hashes are compared to verify\n")
        f.write("that the received file is IDENTICAL to the original.\n\n")
        f.write("KEY CONCEPTS:\n")
        f.write("  - Sequence numbers   : every UDP packet gets a number\n")
        f.write("  - ACKs               : receiver confirms each packet\n")
        f.write("  - Sliding window     : multiple packets in flight\n")
        f.write("  - Selective repeat   : only lost packets retransmitted\n")
        f.write("  - Packet loss sim.   : random.random() < LOSS_RATE\n\n")
        # Pad to ~3 KB so it splits into multiple packets for the demo
        for i in range(2500):
            f.write(f"Line {i+1:>3}: The quick brown fox jumps over the lazy dog. " * 2 + "\n")
    print(f"Created: {txt_path}")
else:
    print(f"Exists : {txt_path}")


# =============================================================================
# 2. MINIMAL VALID PDF (sample.pdf)
# =============================================================================
# A real PDF file starts with %PDF-1.x and ends with %%EOF.
# This creates the smallest valid PDF that displays one line of text.

def create_minimal_pdf(path):
    """Creates a minimal valid PDF file."""

    # PDF objects (everything in a PDF is an "object")
    # Object 1: Catalog (root of the document)
    # Object 2: Pages (list of pages)
    # Object 3: Page (single page)
    # Object 4: Content stream (the actual text drawing commands)
    # Object 5: Font resource

    content_stream = (
        b"BT\n"                    # Begin Text block
        b"/F1 18 Tf\n"             # Font F1 (Times-Roman), size 18
        b"72 720 Td\n"             # Move to position x=72, y=720
        b"(CN Project - P2P File Sharing Demo) Tj\n"
        b"72 690 Td\n"
        b"(TCP + UDP Selective Repeat Protocol) Tj\n"
        b"72 660 Td\n"
        b"(This is a sample PDF used for file transfer demo.) Tj\n"
        b"72 630 Td\n"
        b"(Transfer this file using TCP or UDP and verify SHA-256.) Tj\n"
        b"ET\n"                    # End Text block
    )

    # Build the PDF byte string
    pdf = b"%PDF-1.4\n"

    offsets = []  # Cross-reference table offsets

    def add_object(obj_id, content_bytes):
        """Adds one PDF object and records its byte offset."""
        offsets.append(len(pdf_parts))
        return (f"{obj_id} 0 obj\n").encode() + content_bytes + b"\nendobj\n"

    pdf_parts = [b"%PDF-1.4\n"]

    # Object 1: Catalog
    offsets.append(len(b"".join(pdf_parts)))
    pdf_parts.append(
        b"1 0 obj\n"
        b"<< /Type /Catalog /Pages 2 0 R >>\n"
        b"endobj\n"
    )

    # Object 2: Pages
    offsets.append(len(b"".join(pdf_parts)))
    pdf_parts.append(
        b"2 0 obj\n"
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>\n"
        b"endobj\n"
    )

    # Object 3: Page
    offsets.append(len(b"".join(pdf_parts)))
    pdf_parts.append(
        b"3 0 obj\n"
        b"<< /Type /Page /Parent 2 0 R\n"
        b"   /MediaBox [0 0 612 792]\n"
        b"   /Contents 4 0 R\n"
        b"   /Resources << /Font << /F1 5 0 R >> >> >>\n"
        b"endobj\n"
    )

    # Object 4: Content stream
    offsets.append(len(b"".join(pdf_parts)))
    stream_len = len(content_stream)
    pdf_parts.append(
        f"4 0 obj\n<< /Length {stream_len} >>\nstream\n".encode()
        + content_stream
        + b"\nendstream\nendobj\n"
    )

    # Object 5: Font (Times-Roman, built-in PDF font)
    offsets.append(len(b"".join(pdf_parts)))
    pdf_parts.append(
        b"5 0 obj\n"
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Times-Roman >>\n"
        b"endobj\n"
    )

    # Cross-reference table
    xref_offset = len(b"".join(pdf_parts))
    xref  = b"xref\n"
    xref += f"0 {len(offsets) + 1}\n".encode()
    xref += b"0000000000 65535 f \n"
    for off in offsets:
        xref += f"{off:010d} 00000 n \n".encode()

    pdf_parts.append(xref)
    pdf_parts.append(
        b"trailer\n"
        + f"<< /Size {len(offsets) + 1} /Root 1 0 R >>\n".encode()
        + b"startxref\n"
        + f"{xref_offset}\n".encode()
        + b"%%EOF\n"
    )

    with open(path, "wb") as f:
        f.write(b"".join(pdf_parts))


pdf_path = os.path.join(SHARED_FOLDER, f"{prefix}_report.pdf")
create_minimal_pdf(pdf_path)
pdf_size = os.path.getsize(pdf_path)
print(f"Created: {pdf_path}  ({pdf_size} bytes)")


# =============================================================================
# 3. VALID PNG IMAGE (sample_image.png)
# =============================================================================
# A PNG file has:
#   - 8-byte signature
#   - IHDR chunk (image header: width, height, bit depth, colour type)
#   - IDAT chunk (compressed pixel data)
#   - IEND chunk (end of file marker)
#
# Each chunk = [4-byte length][4-byte type][data][4-byte CRC]

def make_png_chunk(chunk_type, data):
    """Builds one PNG chunk: length + type + data + CRC."""
    chunk_body = chunk_type + data
    crc        = zlib.crc32(chunk_body) & 0xFFFFFFFF   # 32-bit CRC
    return struct.pack(">I", len(data)) + chunk_body + struct.pack(">I", crc)

def create_png(path, width=64, height=64):
    """
    Creates a valid PNG image with a simple gradient pattern.
    Width and height default to 64x64 pixels (RGB colour).
    """
    # PNG signature (8 bytes – identifies the file as PNG)
    signature = b"\x89PNG\r\n\x1a\n"

    # IHDR: image header
    # Format: width(4) height(4) bit_depth(1) colour_type(1) compress(1) filter(1) interlace(1)
    # colour_type=2 → RGB (3 channels, no alpha)
    ihdr_data  = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    ihdr_chunk = make_png_chunk(b"IHDR", ihdr_data)

    # Build raw pixel rows for IDAT
    # Each row starts with a filter byte (0 = no filter), then RGB values per pixel
    raw_rows = b""
    for y in range(height):
        row = b"\x00"    # filter byte = 0 (no filter)
        for x in range(width):
            r = (x * 4) % 256         # Red   channel
            g = (y * 4) % 256         # Green channel
            b_val = (x + y) % 256     # Blue  channel
            row += bytes([r, g, b_val])
        raw_rows += row

    # Compress the pixel data with zlib (PNG requires zlib compression)
    compressed = zlib.compress(raw_rows, level=9)
    idat_chunk = make_png_chunk(b"IDAT", compressed)

    # IEND: empty end-of-file chunk (required by PNG spec)
    iend_chunk = make_png_chunk(b"IEND", b"")

    with open(path, "wb") as f:
        f.write(signature + ihdr_chunk + idat_chunk + iend_chunk)

png_path = os.path.join(SHARED_FOLDER, f"{prefix}_image.png")
create_png(png_path, width=64, height=64)
png_size = os.path.getsize(png_path)
print(f"Created: {png_path}  ({png_size} bytes)")


# =============================================================================
# 4. DUMMY VIDEO FILE (sample_video.mp4)
# =============================================================================
# Real MP4 files are complex (H.264 video + AAC audio + MP4 container).
# For a transfer DEMO, we only need a binary file of realistic size.
# This creates a ~512 KB binary file with a real MP4 "ftyp" box header
# so most tools will recognise it as an MP4 file.
#
# The content after the header is patterned binary data (not real video).
# It is only used to test that large binary files transfer correctly.

def create_dummy_mp4(path, size_kb=512):
    """
    Creates a dummy .mp4 file of approximately size_kb kilobytes.
    The file starts with a real MP4 'ftyp' box so it's identified as MP4.
    The rest of the data is patterned binary (not real video).
    """
    # MP4 'ftyp' box (File Type Box) – tells media players this is MP4
    # Structure: [4-byte box size][4-byte 'ftyp'][major brand][version][compatible brands]
    ftyp_data = (
        b"isom"   # major brand  (ISO Base Media)
        b"\x00\x00\x00\x00"  # minor version
        b"isom"   # compatible brand 1
        b"iso2"   # compatible brand 2
        b"avc1"   # compatible brand 3 (H.264)
        b"mp41"   # compatible brand 4
    )
    ftyp_size = 4 + 4 + len(ftyp_data)        # total box size in bytes
    ftyp_box  = struct.pack(">I", ftyp_size) + b"ftyp" + ftyp_data

    # Fill the rest with patterned binary data to reach target size
    target_bytes = size_kb * 1024
    remaining    = target_bytes - len(ftyp_box)

    # Simple repeating pattern: bytes 0x00 through 0xFF cycling
    pattern = bytes(range(256)) * 4             # 1 KB base pattern
    padding = (pattern * (remaining // len(pattern) + 1))[:remaining]

    with open(path, "wb") as f:
        f.write(ftyp_box + padding)

mp4_path = os.path.join(SHARED_FOLDER, f"{prefix}_video.mp4")
create_dummy_mp4(mp4_path, size_kb=512)
mp4_size = os.path.getsize(mp4_path)
print(f"Created: {mp4_path}  ({mp4_size // 1024} KB)")


# =============================================================================
# Summary
# =============================================================================
print()
print("=" * 50)
print("  Sample files created in shared/ folder:")
print("=" * 50)
for fname in ["sample.txt", "sample.pdf", "sample_image.png", "sample_video.mp4"]:
    fpath = os.path.join(SHARED_FOLDER, fname)
    if os.path.exists(fpath):
        size = os.path.getsize(fpath)
        if size < 1024:
            size_str = f"{size} B"
        elif size < 1024 * 1024:
            size_str = f"{size // 1024} KB"
        else:
            size_str = f"{size // (1024*1024)} MB"
        print(f"  {fname:<25} {size_str}")
print("=" * 50)
print()
print("Now run:  python peer.py")
