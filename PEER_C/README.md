# Decentralized Multi-threaded P2P File Sharing


## Main features

- No central server: every peer can send and download files.
- TCP and UDP are separate menu choices.
- UDP reliability is implemented at the application layer using:
  - sequence numbers
  - ACKs
  - sliding window
  - individual timeouts
  - selective retransmission
  - simulated packet loss
- Users **never enter chunk numbers**. The UDP protocol automatically splits the file into chunks and transfers every chunk.
- The demo video is 512 KB and uses a 1 KB UDP packet size, so it produces **512 UDP chunks** (well above the requested 100).
- TCP and UDP listeners run in background threads.
- Multiple TCP downloads can run concurrently.
- Multiple UDP downloads can run concurrently.
- A peer can send while it is receiving another file.
- Binary files such as PDF, PNG and MP4 are supported.
- SHA-256 verification is included.

## Project files

```text
p2p_file_sharing/
├── peer.py
├── config.py
├── tcp_transfer.py
├── udp_selective_repeat.py
├── file_utils.py
├── generate_samples.py
└── shared/
```

`received/` is created automatically when the peer starts.

## Running 3 peers on one computer

Make three copies of the folder:

```text
CN NEW PROJECT/
├── PEER_A/
├── PEER_B/
└── PEER_C/
```

Use these settings:

| Peer | TCP | UDP | IP for local testing |
|---|---:|---:|---|
| Peer A | 5000 | 6000 | 127.0.0.1 |
| Peer B | 5001 | 6001 | 127.0.0.1 |
| Peer C | 5002 | 6002 | 127.0.0.1 |

For three different computers, replace the IP addresses in `KNOWN_PEERS` with the actual LAN IPv4 addresses.

## Start each peer

In three terminals:

```powershell
cd PEER_A
python peer.py
```

```powershell
cd PEER_B
python peer.py
```

```powershell
cd PEER_C
python peer.py
```

On every peer choose **1. Start Peer / Enable Receiving** once. This starts the TCP and UDP listeners. There is no separate receiver/chunk option.

## Menu

```text
1. Start Peer / Enable Receiving
2. List My Shared Files
3. List Files Available By Peer
4. TCP – Send File
5. TCP – Download File(s) Concurrently
6. UDP – Send File (Selective Repeat)
7. UDP – Download File(s) Concurrently
8. Verify Files (SHA-256)
9. Exit
```

### TCP send

Choose option 4, select a file and select the destination peer. The transfer runs in a background thread.

### TCP download

Choose option 5. Enter how many files you want to download concurrently, then choose a source peer and filename for each job. The program creates one thread per download.

Example:

```text
Download 1 → Peer B → Peer_B_video.mp4
Download 2 → Peer C → Peer_C_report.pdf
```

Both downloads run at the same time.

### UDP send

Choose option 6. Select a file and destination peer. The program automatically:

1. announces the filename;
2. splits the file into 1 KB chunks;
3. assigns sequence numbers;
4. sends up to `WINDOW_SIZE` chunks at a time;
5. receives ACKs;
6. retransmits only timed-out chunks;
7. continues until every chunk is acknowledged;
8. reassembles the file at the receiver.

There is **no chunk-number input**.

### UDP download

Choose option 7. Select the source peer and filename. The downloader sends a file request to the source. The source then starts the normal UDP Selective Repeat sender. The receiver's background UDP listener handles the chunks automatically.

You can request multiple files from different peers in the same menu; one thread is created per download request.

## Important UDP configuration

```python
PACKET_SIZE = 1024
WINDOW_SIZE = 8
TIMEOUT = 1
LOSS_RATE = 0.10
```

With the included 512 KB demo video:

```text
512 KB / 1 KB = 512 chunks
```

So the demo visibly exercises hundreds of sequence numbers rather than only two.

## What to explain in the viva

### TCP

TCP is reliable and connection-oriented. The operating system handles sequencing, acknowledgements, retransmission, flow control and congestion control.

### UDP + Selective Repeat

UDP itself does not provide reliability. This project implements reliability at the application layer. Each chunk has a sequence number, the receiver sends an ACK, and the sender retransmits only the chunk whose timer expires.

### Sliding window

With `WINDOW_SIZE = 8`, the sender can have eight unacknowledged chunks in flight. When ACKs arrive, the window moves forward.

### Concurrent downloads

Each requested download runs in a separate Python thread. The TCP and UDP listener threads also run in the background, allowing a peer to send and receive at the same time.
