#!/usr/bin/env python3

"""Record arbitrary PostgreSQL wire packets after startup completes."""

import argparse
import concurrent.futures
import select
import socket
import struct
from pathlib import Path


def read_exact(stream: socket.socket, length: int) -> bytes:
    chunks = []
    remaining = length
    while remaining:
        chunk = stream.recv(remaining)
        if not chunk:
            raise EOFError("connection closed")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_packet(stream: socket.socket) -> tuple[bytes, bytes]:
    kind = read_exact(stream, 1)
    length_bytes = read_exact(stream, 4)
    length = struct.unpack("!I", length_bytes)[0]
    body = read_exact(stream, length - 4)
    return kind, body


def send_packet(stream: socket.socket, kind: bytes, body: bytes) -> None:
    stream.sendall(kind + struct.pack("!I", len(body) + 4) + body)


def relay_startup(backend: socket.socket, frontend: socket.socket) -> None:
    startup_length_bytes = read_exact(frontend, 4)
    startup_length = struct.unpack("!I", startup_length_bytes)[0]
    startup_body = read_exact(frontend, startup_length - 4)
    negotiation_requests = {
        struct.pack("!I", 80877103),  # SSLRequest
        struct.pack("!I", 80877104),  # GSSENCRequest
    }
    while startup_body in negotiation_requests:
        frontend.sendall(b"N")
        startup_length_bytes = read_exact(frontend, 4)
        startup_length = struct.unpack("!I", startup_length_bytes)[0]
        startup_body = read_exact(frontend, startup_length - 4)
    backend.sendall(startup_length_bytes + startup_body)
    while True:
        kind, body = read_packet(backend)
        send_packet(frontend, kind, body)
        if kind == b"Z":
            return


def record_connection(
    frontend: socket.socket,
    backend_port: int,
) -> list[tuple[bytes, bytes, bytes]]:
    events = []
    with frontend, socket.create_connection(("127.0.0.1", backend_port)) as backend:
        relay_startup(backend, frontend)
        while True:
            readable, _, _ = select.select([frontend, backend], [], [])
            if frontend in readable:
                try:
                    kind, body = read_packet(frontend)
                except EOFError:
                    break
                send_packet(backend, kind, body)
                events.append((b"F", kind, body))
                if kind == b"X":
                    break
            if backend in readable:
                try:
                    kind, body = read_packet(backend)
                except EOFError:
                    break
                send_packet(frontend, kind, body)
                events.append((b"B", kind, body))
    return events


def record(
    listen_port: int,
    backend_port: int,
    connections: int,
) -> list[tuple[bytes, bytes, bytes]]:
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", listen_port))
        listener.listen(connections)
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=connections,
        ) as executor:
            futures = []
            for _ in range(connections):
                frontend, _ = listener.accept()
                futures.append(
                    executor.submit(record_connection, frontend, backend_port)
                )
            connection_events = [future.result() for future in futures]

    events = []
    for connection, connection_entries in enumerate(connection_events, 1):
        print(
            f"connection={connection} start_event={len(events)}",
            flush=True,
        )
        events.extend(connection_entries)
    return events


def write_trace(path: Path, events: list[tuple[bytes, bytes, bytes]]) -> None:
    output = bytearray(b"MGR2")
    output.extend(struct.pack("!I", len(events)))
    for direction, kind, body in events:
        output.extend(direction)
        output.extend(kind)
        output.extend(struct.pack("!I", len(body)))
        output.extend(body)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(output)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen-port", type=int, required=True)
    parser.add_argument("--backend-port", type=int, required=True)
    parser.add_argument("--connections", type=int, default=1)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.connections < 1:
        parser.error("--connections must be positive")
    write_trace(
        args.output,
        record(args.listen_port, args.backend_port, args.connections),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
