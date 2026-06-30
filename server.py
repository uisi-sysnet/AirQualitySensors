import socket
import threading

from parser import process_frame

from config import (
    SERVER_HOST,
    SERVER_PORT,
    BUFFER_SIZE,
    MAX_CONNECTIONS,
)


# ==========================================================
# ACK RESPONSE
# ==========================================================
def build_ack():
    """
    Generic HJ212 ACK.
    Later we'll improve this to generate a proper QN-specific ACK.
    """
    return "##0006QN=ACK;CN=9014;ST=91;CP=&&QnRtn=1&&FFFF"


# ==========================================================
# FRAME EXTRACTOR
# ==========================================================
def extract_frames(buffer: str):
    """
    Extract complete HJ212 frames from a TCP stream.

    Returns:
        frames
        remaining_buffer
    """

    frames = []

    while True:

        start = buffer.find("##")

        if start == -1:
            break

        next_start = buffer.find("##", start + 2)

        if next_start == -1:
            break

        frame = buffer[start:next_start]

        frames.append(frame.strip())

        buffer = buffer[next_start:]

    return frames, buffer


# ==========================================================
# CLIENT THREAD
# ==========================================================
def handle_client(conn, addr):

    ip_address = addr[0]

    print(f"\n[+] Device Connected : {ip_address}")

    buffer = ""

    while True:

        try:

            data = conn.recv(BUFFER_SIZE)

            if not data:
                break

            buffer += data.decode(errors="ignore")

            frames, buffer = extract_frames(buffer)

            for frame in frames:

                parsed = process_frame(frame, ip_address)

                # Device requests ACK
                if "CN=9011" in frame or "CN=9012" in frame:
                    conn.sendall(build_ack().encode())

        except ConnectionResetError:
            print(f"[!] Connection reset by {ip_address}")
            break

        except Exception as e:
            print(f"[ERROR] {e}")
            break

    conn.close()

    print(f"[-] Device Disconnected : {addr}")


# ==========================================================
# START SERVER
# ==========================================================
def start_server():

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    server.bind((SERVER_HOST, SERVER_PORT))

    server.listen(MAX_CONNECTIONS)

    print("=" * 60)
    print("HJ212 TCP SERVER")
    print("=" * 60)
    print(f"Listening : {SERVER_HOST}:{SERVER_PORT}")
    print("=" * 60)

    while True:

        conn, addr = server.accept()

        thread = threading.Thread(
            target=handle_client,
            args=(conn, addr),
            daemon=True
        )

        thread.start()


# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":
    start_server()