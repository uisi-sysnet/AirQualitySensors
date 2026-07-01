from pymodbus.client import ModbusTcpClient
from pymodbus.framer import FramerType

import threading
import time

from config import (
    STATIONS,
    LEAD_POLL_INTERVAL,
)

from database import update_lead_value


# ==========================================================
# Read Holding Registers
# ==========================================================
def read_registers(client, address, count, slave):

    try:
        return client.read_holding_registers(
            address=address,
            count=count,
            device_id=slave
        )

    except TypeError:
        return client.read_holding_registers(
            address=address,
            count=count,
            slave=slave
        )


# ==========================================================
# Poll One Sensor
# ==========================================================
def poll_station(mn, station):

    ip = station["lead_ip"]
    port = station["lead_port"]
    slave = station["lead_slave"]

    client = ModbusTcpClient(
        host=ip,
        port=port,
        framer=FramerType.RTU,
        timeout=3,
    )

    if not client.connect():

        print(f"[LEAD] Cannot connect {ip}")

        return

    try:

        rr = read_registers(client, 0, 10, slave)

        if rr.isError():

            print(f"[LEAD] Read Error {ip}")

            return

        regs = rr.registers

        lead = regs[2] / 10.0

        temperature = regs[1] / 10.0

        print(f"[LEAD] {mn}  Lead={lead:.2f} ppm")

        update_lead_value(
            mn=mn,
            lead=lead,
            temperature=temperature,
        )

    except Exception as e:

        print(f"[LEAD ERROR] {ip} : {e}")

    finally:

        client.close()


# ==========================================================
# Background Thread
# ==========================================================
def lead_service():

    print("Lead Sensor Service Started")

    while True:

        for mn, station in STATIONS.items():

            if not station["enabled"]:
                continue

            poll_station(mn, station)

        time.sleep(LEAD_POLL_INTERVAL)


# ==========================================================
# Start Thread
# ==========================================================
def start_lead_service():

    thread = threading.Thread(
        target=lead_service,
        daemon=True,
        name="LeadSensor"
    )

    thread.start()