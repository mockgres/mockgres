#!/usr/bin/env python3

"""Record simple-query PostgreSQL wire responses for a regression script."""

import argparse
import concurrent.futures
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
    return kind, length_bytes + body


def relay_until_ready(
    backend: socket.socket,
    frontend: socket.socket,
    capture: bool,
) -> list[tuple[bytes, bytes]]:
    messages = []
    while True:
        kind, packet = read_packet(backend)
        frontend.sendall(kind + packet)
        if capture:
            messages.append((kind, packet[4:]))
        if kind == b"G":
            while True:
                frontend_kind, frontend_packet = read_packet(frontend)
                backend.sendall(frontend_kind + frontend_packet)
                if frontend_kind in (b"c", b"f"):
                    break
        if kind == b"Z":
            return messages


def record_connection(
    frontend: socket.socket,
    backend_port: int,
) -> list[tuple[bytes, list[tuple[bytes, bytes]]]]:
    entries = []
    with frontend, socket.create_connection(("127.0.0.1", backend_port)) as backend:
        startup_length_bytes = read_exact(frontend, 4)
        startup_length = struct.unpack("!I", startup_length_bytes)[0]
        startup_body = read_exact(frontend, startup_length - 4)
        negotiation_requests = {
            struct.pack("!I", 80877103),  # SSLRequest
            struct.pack("!I", 80877104),  # GSSENCRequest
        }
        while startup_body in negotiation_requests:
            frontend.sendall(b"N")
            try:
                startup_length_bytes = read_exact(frontend, 4)
            except EOFError:
                return entries
            startup_length = struct.unpack("!I", startup_length_bytes)[0]
            startup_body = read_exact(frontend, startup_length - 4)
        backend.sendall(startup_length_bytes + startup_body)
        relay_until_ready(backend, frontend, False)

        while True:
            try:
                kind, packet = read_packet(frontend)
            except EOFError:
                break
            backend.sendall(kind + packet)
            if kind == b"X":
                break
            if kind != b"Q":
                raise RuntimeError(f"unsupported frontend message: {kind!r}")
            query = packet[4:].rstrip(b"\0")
            messages = relay_until_ready(backend, frontend, True)
            entries.append((query, messages))
    return entries


def record(
    listen_port: int,
    backend_port: int,
    connections: int,
) -> list[tuple[bytes, list[tuple[bytes, bytes]]]]:
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", listen_port))
        listener.listen(1)
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=connections,
        ) as executor:
            futures = []
            for _ in range(connections):
                frontend, _ = listener.accept()
                futures.append(
                    executor.submit(record_connection, frontend, backend_port)
                )
            connections_entries = [future.result() for future in futures]

    entries = []
    for connection, connection_entries in enumerate(connections_entries, 1):
        print(
            f"connection={connection} start_entry={len(entries)}",
            flush=True,
        )
        entries.extend(connection_entries)
    return entries


def write_trace(path: Path, entries: list[tuple[bytes, list[tuple[bytes, bytes]]]]) -> None:
    output = bytearray(b"MGR1")
    output.extend(struct.pack("!I", len(entries)))
    for query, messages in entries:
        output.extend(struct.pack("!I", len(query)))
        output.extend(query)
        output.extend(struct.pack("!I", len(messages)))
        for kind, body in messages:
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
