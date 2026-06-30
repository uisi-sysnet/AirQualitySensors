"""
hj212.py
HJ/T 212 Protocol Helper Functions

This module handles:
- Frame extraction
- Header field extraction
- CRC16 calculation
- ACK generation
"""

import re

# ==========================================================
# CRC16 (HJ212 / Modbus)
# ==========================================================
def crc16(data: str) -> str:
    """
    Calculate CRC16 (Modbus) checksum.

    Returns:
        4-character uppercase hexadecimal string.
    """

    crc = 0xFFFF

    for b in data.encode("ascii"):

        crc ^= b

        for _ in range(8):

            if crc & 0x0001:
                crc >>= 1
                crc ^= 0xA001
            else:
                crc >>= 1

    return f"{crc:04X}"


# ==========================================================
# VERIFY CRC
# ==========================================================
def verify_crc(frame: str) -> bool:
    """
    Verify CRC of an HJ212 frame.

    Frame format:
        ##LLLL<body>CCCC

    where:
        LLLL = length
        CCCC = CRC16
    """

    try:

        if not frame.startswith("##"):
            return False

        body = frame[6:-4]

        received_crc = frame[-4:].upper()

        calculated_crc = crc16(body)

        return received_crc == calculated_crc

    except Exception:

        return False


# ==========================================================
# GET HEADER FIELD
# ==========================================================
def get_field(frame: str, field: str) -> str:
    """
    Extract a header field.

    Example:
        get_field(frame, "QN")
        get_field(frame, "MN")
    """

    match = re.search(rf"{field}=([^;]+)", frame)

    if match:
        return match.group(1)

    return ""


# ==========================================================
# EXTRACT COMPLETE FRAMES
# ==========================================================
def extract_frames(buffer: str):
    """
    Extract complete HJ212 frames from TCP stream.

    Returns:
        frames
        remaining_buffer
    """

    frames = []

    while True:

        start = buffer.find("##")

        if start == -1:
            break

        if len(buffer) < start + 6:
            break

        try:

            length = int(buffer[start + 2:start + 6])

        except ValueError:

            buffer = buffer[start + 2:]
            continue

        total_length = 2 + 4 + length + 4

        if len(buffer) < start + total_length:
            break

        frame = buffer[start:start + total_length]

        frames.append(frame)

        buffer = buffer[start + total_length:]

    return frames, buffer


# ==========================================================
# BUILD ACK
# ==========================================================
def build_ack(frame: str) -> str:
    """
    Build HJ212 ACK frame.
    """

    qn = get_field(frame, "QN")
    pw = get_field(frame, "PW")
    mn = get_field(frame, "MN")

    body = (
        f"QN={qn};"
        f"ST=91;"
        f"CN=9014;"
        f"PW={pw};"
        f"MN={mn};"
        f"Flag=4;"
        f"CP=&&QnRtn=1;ExeRtn=1&&"
    )

    length = f"{len(body):04d}"

    crc = crc16(body)

    return f"##{length}{body}{crc}\r\n"