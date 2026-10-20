
import socket
import threading
import os

PORT = 5050
BUFFER_SIZE = 64 * 1024
HEADER = 64
DOWNLOAD_DIR = "received_files"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

send_lock = threading.Lock()
active_connections = []
connections_lock = threading.Lock()


def recv_exact(conn, size):
    data = bytearray()

    while len(data) < size:
        chunk = conn.recv(min(BUFFER_SIZE, size - len(data)))
        if not chunk:
            return None
        data.extend(chunk)

    return bytes(data)


def receive_files(conn, addr):
    print(f"\n✅ Connected: {addr}")

    try:
        while True:
            header = recv_exact(conn, HEADER)
            if header is None:
                break

            try:
                filename_length, file_size = map(
                    int, header.decode().strip().split()
                )
            except ValueError:
                print("Invalid file header.")
                break

            if not 1 <= filename_length <= 4096 or file_size < 0:
                print("Invalid file information.")
                break

            filename_data = recv_exact(conn, filename_length)
            if filename_data is None:
                break

            filename = os.path.basename(
                filename_data.decode("utf-8")
            ) or "received_file.bin"

            filepath = os.path.join(DOWNLOAD_DIR, filename)
            base, ext = os.path.splitext(filename)
            counter = 1

            while os.path.exists(filepath):
                filepath = os.path.join(
                    DOWNLOAD_DIR, f"{base}_{counter}{ext}"
                )
                counter += 1

            remaining = file_size

            with open(filepath, "wb") as f:
                while remaining:
                    chunk = conn.recv(min(BUFFER_SIZE, remaining))
                    if not chunk:
                        raise ConnectionError("Transfer interrupted")
                    f.write(chunk)
                    remaining -= len(chunk)

            print(f"\n📥 Received: {filepath} ({file_size} bytes)")

    except (OSError, ConnectionError, UnicodeDecodeError) as e:
        print(f"\nConnection error: {e}")

    finally:
        with connections_lock:
            if conn in active_connections:
                active_connections.remove(conn)
        conn.close()
        print(f"\nDisconnected: {addr}")


def accept_connections():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", PORT))
    server.listen()

    print(f"📡 Listening on port {PORT}")

    while True:
        conn, addr = server.accept()

        with connections_lock:
            active_connections.append(conn)

        threading.Thread(
            target=receive_files,
            args=(conn, addr),
            daemon=True
        ).start()


def send_file(conn, path):
    if not os.path.isfile(path):
        print("❌ File not found.")
        return

    filename_data = os.path.basename(path).encode("utf-8")
    file_size = os.path.getsize(path)

    header = f"{len(filename_data)} {file_size}".encode().ljust(
        HEADER, b" "
    )

    try:
        with send_lock:
            conn.sendall(header)
            conn.sendall(filename_data)

            with open(path, "rb") as f:
                while True:
                    chunk = f.read(BUFFER_SIZE)
                    if not chunk:
                        break
                    conn.sendall(chunk)

        print(f"📤 Sent: {os.path.basename(path)}")

    except OSError as e:
        print(f"❌ Send failed: {e}")


def main():
    threading.Thread(
        target=accept_connections,
        daemon=True
    ).start()

    print("\n=== PEER-TO-PEER FILE TRANSFER ===")
    print("Commands:")
    print("connect <IP>        Connect to another computer")
    print('send <connection> "<file path>"')
    print("peers               List connections")
    print("exit                Quit")

    while True:
        try:
            command = input("\n> ").strip()

            if command == "exit":
                break

            elif command == "peers":
                with connections_lock:
                    for i, conn in enumerate(active_connections):
                        print(i, conn.getpeername())

            elif command.startswith("connect "):
                ip = command[8:].strip()

                try:
                    conn = socket.socket(
                        socket.AF_INET, socket.SOCK_STREAM
                    )
                    conn.connect((ip, PORT))

                    with connections_lock:
                        active_connections.append(conn)

                    threading.Thread(
                        target=receive_files,
                        args=(conn, conn.getpeername()),
                        daemon=True
                    ).start()

                    print(f"✅ Connected to {ip}")

                except OSError as e:
                    print(f"❌ Could not connect: {e}")

            elif command.startswith("send "):
                parts = command.split(maxsplit=2)

                if len(parts) < 3:
                    print('Usage: send <connection> "<file path>"')
                    continue

                try:
                    index = int(parts[1])
                    path = parts[2].strip('"')

                    with connections_lock:
                        conn = active_connections[index]

                    send_file(conn, path)

                except (ValueError, IndexError):
                    print("Invalid connection number.")

            else:
                print("Unknown command.")

        except (KeyboardInterrupt, EOFError):
            break

    with connections_lock:
        for conn in active_connections:
            conn.close()

    print("👋 Goodbye!")


if __name__ == "__main__":
    main()
