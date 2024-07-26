#!/usr/bin/python3

import sys
import socket

# Configuration
SOCKET_FILE = "/tmp/pytarchive_service.sock"


def send_command(command: bytes) -> str:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client_socket:
        try:
            client_socket.connect(SOCKET_FILE)
        except PermissionError:
            print("You need to be root to run this script.")
            exit()
        client_socket.sendall(command)
        response = recv_all(client_socket)
        print(response.decode())


def recv_all(sock):
    """Helper function to receive all data from a socket."""
    data = b""
    while True:
        part = sock.recv(1024)
        data += part
        if len(part) < 1024:
            break
    return data


def main():
    if len(sys.argv) < 2:
        print("Usage: pytarchive <command> [<args>]")
        sys.exit(1)

    command = b"\0".join(i.encode() for i in sys.argv[1:])
    send_command(command)


if __name__ == "__main__":
    main()
