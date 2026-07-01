#!/usr/bin/env python3
"""
HJ212 & Modbus Air Quality Data Collection Server (Unified Production Script)
Features File Logging & Multi-Device Scalability Support
"""

import datetime
import logging
import re
import socket
import threading
import time
from datetime import datetime

import psycopg2
from psycopg2 import pool
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import RealDictCursor
from pymodbus.client import ModbusTcpClient
from pymodbus.framer import FramerType

# ==========================================================
# 1. CONFIGURATION (Formerly config.py)
# ==========================================================
SERVER_HOST = "0.0.0.0"
SERVER_PORT = 1935
BUFFER_SIZE = 4096
MAX_CONNECTIONS = 20
VERIFY_CHECKSUM = False  # Set to True once CRC validation is required
SUPPORTED_CN = ["2011", "9014"]  # Core command numbers that trigger an ACK
LEAD_POLL_INTERVAL = 30          # Modbus polling loop timer in seconds

# File Logging Config
LOG_FILE_NAME = "hj212_server.log"

# Database Connection Variables
DB_HOST = "127.0.0.1"
DB_PORT = 5432
DB_NAME = "air_quality"
DB_USER = "aq_user"
DB_PASSWORD = "UisI_2026##"

# Registered Station Records (Add as many devices as you need here)
STATIONS = {
    "4101025U122041": {
        "station_name": "Station 1",
        "enabled": True,
        "lead_ip": "192.168.55.11",
        "lead_port": 8899,
        "lead_slave": 1,
    },
    "4101025U122042": {
        "station_name": "Station 2",
        "enabled": True,
        "lead_ip": "192.168.55.12",
        "lead_port": 8899,
        "lead_slave": 1,
    }
}

# Setup Global Dual Logging Context (Consoles + Local Text File)
logger = logging.getLogger("combined_server")
logger.setLevel(logging.INFO)

log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(threadName)s: %(message)s")

# Console Output Handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# File Persistent Record Storage Handler
file_handler = logging.FileHandler(LOG_FILE_NAME, encoding="utf-8")
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)


# ==========================================================
# 2. SENSOR DEFINITIONS & UTILITIES (Formerly sensors.py)
# ==========================================================
SENSORS = {
    "a34004": {"name": "PM2.5", "column": "pm25", "unit": "µg/m³"},
    "a34002": {"name": "PM10", "column": "pm10", "unit": "µg/m³"},
    "a34001": {"name": "TSP", "column": "tsp", "unit": "µg/m³"},
    "a05024": {"name": "Ozone", "column": "ozone", "unit": "µg/m³"},
    "a21005": {"name": "Carbon Monoxide", "column": "carbon_monoxide", "unit": "mg/m³"},
    "a21026": {"name": "Sulfur Dioxide", "column": "sulfur_dioxide", "unit": "µg/m³"},
    "a21004": {"name": "Nitrogen Dioxide", "column": "nitrogen_dioxide", "unit": "µg/m³"},
    "a01001": {"name": "Temperature", "column": "temperature", "unit": "°C"},
    "a01002": {"name": "Humidity", "column": "humidity", "unit": "%"},
    "a06001": {"name": "Rain", "column": "rain", "unit": "mm"},
    "LA":     {"name": "Noise", "column": "noise", "unit": "dB"},
    "a01007": {"name": "Wind Speed", "column": "wind_speed", "unit": "m/s"},
    "a01008": {"name": "Wind Direction", "column": "wind_direction", "unit": "°"},
    "a01006": {"name": "Air Pressure", "column": "air_pressure", "unit": "kPa"},
}

SENSOR_MAP = {code: sensor["name"] for code, sensor in SENSORS.items()}

def get_sensor(code):
    return SENSORS.get(code)

def get_sensor_name(code):
    sensor = get_sensor(code)
    return sensor["name"] if sensor else code

def get_database_column(code):
    sensor = get_sensor(code)
    return sensor["column"] if sensor else None

def get_unit(code):
    sensor = get_sensor(code)
    return sensor["unit"] if sensor else ""


# ==========================================================
# 3. STATION REGISTRY HELPERS (Formerly station.py)
# ==========================================================
def get_station(mn):
    return STATIONS.get(mn)

def get_lead_ip(mn):
    station = STATIONS.get(mn)
    return station["lead_ip"] if station else None

def get_slave(mn):
    station = STATIONS.get(mn)
    return station["lead_slave"] if station else 1


# ==========================================================
# 4. DATABASE LAYER (Formerly database.py)
# ==========================================================
_connection_pool = None
_pool_lock = threading.Lock()

def create_database_if_not_exists():
    conn = None
    try:
        logger.info(f"Checking database '{DB_NAME}'...")
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database="postgres", user=DB_USER, password=DB_PASSWORD
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
        exists = cur.fetchone()

        if exists:
            logger.info(f"Database '{DB_NAME}' already exists.")
        else:
            logger.info(f"Creating database '{DB_NAME}'...")
            cur.execute(f'CREATE DATABASE "{DB_NAME}"')
            logger.info("Database created successfully.")
        cur.close()
        return True
    except Exception as e:
        logger.exception(f"Unable to create database: {e}")
        return False
    finally:
        if conn:
            conn.close()

def initialize_database():
    global _connection_pool
    with _pool_lock:
        if _connection_pool is not None:
            return True
        if not create_database_if_not_exists():
            return False
        try:
            _connection_pool = pool.ThreadedConnectionPool(
                minconn=2, maxconn=20, host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
            )
            logger.info("Connected to PostgreSQL successfully.")
            create_tables()
            return True
        except Exception as e:
            logger.exception(f"Database initialization failed: {e}")
            return False

def get_connection():
    global _connection_pool
    if _connection_pool is None:
        raise RuntimeError("Database pool is not initialized.")
    return _connection_pool.getconn()

def release_connection(conn):
    global _connection_pool
    if conn is None:
        return
    try:
        _connection_pool.putconn(conn)
    except Exception:
        conn.close()

def close_database():
    global _connection_pool
    if _connection_pool:
        logger.info("Closing database connection pool...")
        _connection_pool.closeall()
        _connection_pool = None

def create_tables():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS sensor_data (
            id BIGSERIAL PRIMARY KEY,
            station_mn VARCHAR(32) NOT NULL,
            ip_address INET,
            data_time TIMESTAMP,
            pm25 DOUBLE PRECISION,
            pm10 DOUBLE PRECISION,
            tsp DOUBLE PRECISION,
            ozone DOUBLE PRECISION,
            carbon_monoxide DOUBLE PRECISION,
            sulfur_dioxide DOUBLE PRECISION,
            nitrogen_dioxide DOUBLE PRECISION,
            temperature DOUBLE PRECISION,
            humidity DOUBLE PRECISION,
            rain DOUBLE PRECISION,
            wind_speed DOUBLE PRECISION,
            wind_direction DOUBLE PRECISION,
            air_pressure DOUBLE PRECISION,
            noise DOUBLE PRECISION,
            lead DOUBLE PRECISION,
            lead_temperature DOUBLE PRECISION,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sensor_time ON sensor_data(data_time);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_station ON sensor_data(station_mn);")
        conn.commit()
        logger.info("Database schemas and indexes checked.")
    except Exception as e:
        if conn: conn.rollback()
        logger.exception(f"Table creation check failed: {e}")
    finally:
        if conn: release_connection(conn)

def insert_sensor_data(data, ip_address):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cp = data.get("CP", {})
        mn = data.get("MN")
        data_time = None

        if "DataTime" in cp:
            try:
                data_time = datetime.strptime(cp["DataTime"], "%Y%m%d%H%M%S")
            except ValueError:
                logger.warning("Invalid DataTime string parsing format.")

        values = {
            "station_mn": mn,
            "ip_address": ip_address,
            "data_time": data_time,
        }

        for code, sensor in SENSORS.items():
            sensor_name = sensor["name"]
            column = sensor["column"]
            if sensor_name not in cp:
                continue
            sensor_data = cp[sensor_name]
            value = sensor_data.get("Rtd") or sensor_data.get("Avg") or sensor_data.get("Value")
            values[column] = value

        columns = list(values.keys())
        placeholders = ["%s"] * len(columns)
        sql = f"INSERT INTO sensor_data ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        
        cur.execute(sql, [values[col] for col in columns])
        conn.commit()
        logger.info(f"Successfully stored database records for station {mn} ({ip_address})")
    except Exception as e:
        if conn: conn.rollback()
        logger.exception(f"Insert query execution failed: {e}")
    finally:
        if conn: release_connection(conn)

def update_lead_value(mn, lead, temperature):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE sensor_data
            SET lead = %s, lead_temperature = %s
            WHERE id = (
                SELECT id FROM sensor_data WHERE station_mn = %s
                ORDER BY data_time DESC NULLS LAST, created_at DESC LIMIT 1
            )
            """, (lead, temperature, mn)
        )
        conn.commit()
        logger.info(f"Lead variables updated in Database for station: {mn}")
    except Exception as e:
        if conn: conn.rollback()
        logger.exception(f"Asynchronous lead update thread write encountered database error: {e}")
    finally:
        if conn: release_connection(conn)


# ==========================================================
# 5. HJ212 PROTOCOL UTILITIES (Formerly hj212.py)
# ==========================================================
def crc16(data: str) -> str:
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

def verify_crc(frame: str) -> bool:
    try:
        if not frame.startswith("##"):
            return False
        body = frame[6:-4]
        received_crc = frame[-4:].upper()
        calculated_crc = crc16(body)
        return received_crc == calculated_crc
    except Exception:
        return False

def get_field(frame: str, field: str) -> str:
    match = re.search(rf"{field}=([^;]+)", frame)
    return match.group(1) if match else ""

def extract_frames(buffer: str):
    frames = []
    while True:
        start = buffer.find("##")
        if start == -1: break
        if len(buffer) < start + 6: break
        try:
            length = int(buffer[start + 2:start + 6])
        except ValueError:
            buffer = buffer[start + 2:]
            continue
        total_length = 2 + 4 + length + 4
        if len(buffer) < start + total_length: break
        frame = buffer[start:start + total_length]
        frames.append(frame)
        buffer = buffer[start + total_length:]
    return frames, buffer

def build_ack(frame: str) -> str:
    qn = get_field(frame, "QN")
    pw = get_field(frame, "PW")
    mn = get_field(frame, "MN")
    body = f"QN={qn};ST=91;CN=9014;PW={pw};MN={mn};Flag=4;CP=&&QnRtn=1;ExeRtn=1&&"
    length = f"{len(body):04d}"
    crc = crc16(body)
    return f"##{length}{body}{crc}\r\n"


# ==========================================================
# 6. HJ212 FRAME PARSER (Formerly parser.py)
# ==========================================================
def parse_cp(cp_data: str):
    result = {}
    if not cp_data: return result
    sections = cp_data.split(";")
    for section in sections:
        if not section: continue
        if section.startswith("DataTime="):
            result["DataTime"] = section.split("=", 1)[1]
            continue
        values = section.split(",")
        for value in values:
            if "=" not in value: continue
            key, val = value.split("=", 1)
            code, metric = key.split("-", 1) if "-" in key else (key, "Value")
            sensor = SENSOR_MAP.get(code, code)
            if sensor not in result:
                result[sensor] = {}
            try: val = float(val)
            except: pass
            result[sensor][metric] = val
    return result

def parse_frame(frame: str):
    frame = frame.replace("\r", "").replace("\n", "")
    if not frame.startswith("##"): return None
    length = frame[2:6]
    body = frame[6:]
    fields = dict(re.findall(r"(\w+)=([^;]+)", body))
    cp_match = re.search(r"CP=&&(.+?)&&", body)
    cp_data = cp_match.group(1) if cp_match else ""
    return {
        "Length": length, "QN": fields.get("QN"), "ST": fields.get("ST"),
        "CN": fields.get("CN"), "PW": fields.get("PW"), "MN": fields.get("MN"),
        "Flag": fields.get("Flag"), "CP": parse_cp(cp_data), "RawCP": cp_data
    }

def print_frame(data):
    if not data: return
    logger.info(f"Parsed fields -> QN: {data['QN']}, CN: {data['CN']}, MN: {data['MN']}")
    cp = data["CP"]
    for sensor, values in cp.items():
        if sensor == "DataTime": continue
        value = values.get("Rtd") or values.get("Avg") or values.get("Value")
        logger.info(f"--- Metric Metric: {sensor} -> {value}")

def process_frame(frame, ip_address):
    data = parse_frame(frame)
    if not data: return
    print_frame(data)
    insert_sensor_data(data, ip_address)


# ==========================================================
# 7. MODBUS LEAD SENSOR SERVICE (Formerly lead_sensor.py)
# ==========================================================
def read_registers(client, address, count, slave):
    try:
        return client.read_holding_registers(address=address, count=count, device_id=slave)
    except TypeError:
        return client.read_holding_registers(address=address, count=count, slave=slave)

def poll_station(mn, station):
    ip, port, slave = station["lead_ip"], station["lead_port"], station["lead_slave"]
    client = ModbusTcpClient(host=ip, port=port, framer=FramerType.RTU, timeout=3)
    if not client.connect():
        logger.warning(f"[MODBUS WORKER] Unable to reach Modbus gateway at: {ip}")
        return
    try:
        rr = read_registers(client, 0, 10, slave)
        if rr.isError():
            logger.error(f"[MODBUS WORKER] Read register exception payload on IP: {ip}")
            return
        regs = rr.registers
        lead = regs[2] / 10.0
        temperature = regs[1] / 10.0
        logger.info(f"[MODBUS DATA] {mn} ({ip}) -> Lead: {lead:.2f} ppm, Temp: {temperature:.1f}°C")
        update_lead_value(mn=mn, lead=lead, temperature=temperature)
    except Exception as e:
        logger.exception(f"[MODBUS CRITICAL] Exception thrown inside poll chain on {ip}: {e}")
    finally:
        client.close()

def lead_service():
    logger.info("Background Modbus loop scheduler active.")
    while True:
        for mn, station in STATIONS.items():
            if not station["enabled"]: continue
            poll_station(mn, station)
        time.sleep(LEAD_POLL_INTERVAL)

def start_lead_service():
    thread = threading.Thread(target=lead_service, daemon=True, name="ModbusLeadService")
    thread.start()


# ==========================================================
# 8. MAIN SOCKET SERVER (Formerly server.py)
# ==========================================================
def handle_client(conn, addr):
    ip_address = addr[0]
    logger.info(f"[+] Active socket tunnel established with client: {ip_address}")
    buffer = ""
    while True:
        try:
            data = conn.recv(BUFFER_SIZE)
            if not data: break
            buffer += data.decode(errors="ignore")
            frames, buffer = extract_frames(buffer)

            for frame in frames:
                # Every raw packet received is stored permanently in the .log file via logger structure
                logger.info(f"[RAW INBOUND PACKET] Received data: {frame}")
                
                if VERIFY_CHECKSUM and not verify_crc(frame):
                    logger.warning(f"[CRC REFUSED] Data stream frame verification mismatch from source IP: {ip_address}")
                    continue

                cn = get_field(frame, "CN")
                if cn == "2011":
                    try: process_frame(frame, ip_address)
                    except Exception as e: logger.exception(f"[PARSER ISSUE] Runtime thread issue handling telemetry fields: {e}")
                else:
                    logger.info(f"Metric categorization skip. Dropping non-storage CN sequence type: CN={cn}")

                if cn in SUPPORTED_CN:
                    ack = build_ack(frame)
                    conn.sendall(ack.encode())
                    logger.info(f"[ACK TRANSMITTED] Handshake context feedback: {ack.strip()}")

        except socket.timeout: logger.info(f"[CONN TIMEOUT] Communication pipeline inactive: {ip_address}"); break
        except ConnectionResetError: logger.info(f"[CONN DISRUPTED] Pipeline drops cleanly at destination: {ip_address}"); break
        except Exception as e: logger.exception(f"[THREAD ERROR] Client session thread exception safely contained: {e}"); break

    conn.close()
    logger.info(f"[-] Client socket stream terminated cleanly: {ip_address}")

def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((SERVER_HOST, SERVER_PORT))
    server.listen(MAX_CONNECTIONS)
    logger.info(f"HJ212 SERVER BOUND AND ONLINE. Listening on: {SERVER_HOST}:{SERVER_PORT}")

    while True:
        conn, addr = server.accept()
        conn.settimeout(60)
        thread = threading.Thread(target=handle_client, args=(conn, addr), daemon=True, name=f"HJ212-{addr[0]}")
        thread.start()

if __name__ == "__main__":
    if initialize_database():
        start_lead_service()
        start_server()
    else:
        logger.critical("Initialization checks incomplete. Halting execution runtime script.")