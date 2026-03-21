#!/usr/bin/env python3

import json
import socket
import struct
import sys
import time


def read_exact(sock, size):
    chunks = []
    remaining = size
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise RuntimeError("connection closed")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def main():
    if len(sys.argv) != 4:
        raise SystemExit("usage: remote_rpc.py <path> <json-payload> <timeout-seconds>")

    path = sys.argv[1]
    payload = json.loads(sys.argv[2])
    timeout = float(sys.argv[3])

    body = json.dumps({"path": path, "payload": payload}).encode("utf-8")
    frame = struct.pack(">I", len(body)) + body + struct.pack(">I", len(body))

    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        remaining = max(0.05, deadline - time.monotonic())
        try:
            with socket.create_connection(("127.0.0.1", 8000), timeout=remaining) as sock:
                sock.settimeout(remaining)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.sendall(frame)

                header = read_exact(sock, 4)
                size = struct.unpack(">I", header)[0]
                response = read_exact(sock, size)
                marker = read_exact(sock, 4)
                if marker != header:
                    raise RuntimeError("invalid frame marker")
            break
        except OSError as exc:
            last_error = exc
            time.sleep(0.05)
    else:
        raise RuntimeError(f"rpc connect failed: {last_error}")

    sys.stdout.write(response.decode("utf-8"))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)
