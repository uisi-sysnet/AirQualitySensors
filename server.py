import socket
import threading

from parser import process_frame

from database import initialize_database

from hj212 import (
    extract_frames,
    build_ack,
    get_field,
    verify_crc,
)

from config import (
    SERVER_HOST,
    SERVER_PORT,
    BUFFER_SIZE,
    MAX_CONNECTIONS,
    SUPPORTED_CN,
    VERIFY_CHECKSUM,
)

from lead_sensor import start_lead_service

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

                print(f"\n[RX] {frame}")

                # --------------------------------------------------
                # Verify CRC
                # --------------------------------------------------
                if VERIFY_CHECKSUM:
                    
                    if not verify_crc(frame):
                        print(f"[CRC ERROR] Invalid CRC from {ip_address}")
                        continue

                # --------------------------------------------------
                # Get Command Number
                # --------------------------------------------------
                cn = get_field(frame, "CN")

                # --------------------------------------------------
                # Parse and Save
                # --------------------------------------------------
                # Only save measurement data
                if cn == "2011":

                    try:
                        process_frame(frame, ip_address)
                    except Exception as e:
                        print(f"[PARSER ERROR] {e}")

                else:
                    print(f"[INFO] CN={cn} received (not stored)")

                # --------------------------------------------------
                # Send ACK
                # --------------------------------------------------
                if cn in SUPPORTED_CN:

                    ack = build_ack(frame)

                    conn.sendall(ack.encode())

                    print(f"[TX] {ack.strip()}")

        except socket.timeout:
            print(f"[TIMEOUT] {ip_address}")
            break

        except ConnectionResetError:
            print(f"[RESET] {ip_address}")
            break

        except Exception as e:
            print(f"[ERROR] {ip_address}: {e}")
            break

    conn.close()

    print(f"[-] Device Disconnected : {ip_address}")


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

        conn.settimeout(60)

        thread = threading.Thread(
            target=handle_client,
            args=(conn, addr),
            daemon=True,
            name=f"HJ212-{addr[0]}"
        )

        thread.start()


# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":

    if initialize_database():
        start_lead_service()
        start_server()

    else:
         print("Database initialization failed.")
