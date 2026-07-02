#!/usr/bin/env python3
"""
Air Quality Monitoring Server v3.5
Protocol: HJ212 & Modbus TCP
Features: TCP Server, PostgreSQL Pool, Modbus Polling, FastAPI REST API, Time-Series Analytics
"""

import logging
import re
import socket
import threading
import time
from datetime import datetime, timezone, timedelta

import psycopg2
from psycopg2 import pool
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import RealDictCursor
from pymodbus.client import ModbusTcpClient
from pymodbus.framer import FramerType

# FastAPI Imports
import uvicorn
from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.security import APIKeyHeader

# ==========================================================
# CONFIGURATION 
# ==========================================================
SERVER_HOST = "0.0.0.0"
SERVER_PORT = 1935
BUFFER_SIZE = 4096
MAX_CONNECTIONS = 20
VERIFY_CHECKSUM = False             
SUPPORTED_CN = ["2011", "9014"]     
LEAD_POLL_INTERVAL = 30             

# API Configuration
API_PORT = 8000
API_TOKEN = "YourSecureToken123"

# File Logging Config
LOG_FILE_NAME = "hj212_server.log"

# Database Connection Variables
DB_HOST = "127.0.0.1"
DB_PORT = 5432
DB_NAME = "air_quality"
DB_USER = "aq_user"
DB_PASSWORD = "UisI_2026##"

# Registered Station Records
STATIONS = {
    "4101025U122041": {                 
        "station_name": "AQM001",    
        "enabled": True,                
        "latitude": 14.5995,            
        "longitude": 120.9842,          
        "lead_ip": "192.168.55.11",     
        "lead_port": 8899,              
        "lead_slave": 1,                
    },
    "4101025U122042": {
        "station_name": "AQM002",
        "enabled": True,
        "latitude": 14.6095,
        "longitude": 120.9942,
        "lead_ip": "192.168.55.12",
        "lead_port": 8899,
        "lead_slave": 1,
    }
}

# Setup Global Dual Logging Context
logger = logging.getLogger("combined_server")
logger.setLevel(logging.INFO)
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(threadName)s: %(message)s")

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

file_handler = logging.FileHandler(LOG_FILE_NAME, encoding="utf-8")
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)


# ==========================================================
# SENSOR DEFINITIONS & UTILITIES 
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


# ==========================================================
# TIME UTILITY FOR API GENERATION
# ==========================================================
def format_api_datetime(dt: datetime) -> str:
    """Formats a datetime to exact millisecond ISO-8601 UTC string: YYYY-MM-DDTHH:MM:SS.mmmZ"""
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# ==========================================================
# DATABASE LAYER
# ==========================================================
_connection_pool = None
_pool_lock = threading.Lock()

def create_database_if_not_exists():
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database="postgres", user=DB_USER, password=DB_PASSWORD)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
        if not cur.fetchone():
            cur.execute(f'CREATE DATABASE "{DB_NAME}"')
            logger.info(f"Database '{DB_NAME}' created successfully.")
        cur.close()
        return True
    except Exception as e:
        logger.exception(f"Unable to create database: {e}")
        return False
    finally:
        if conn: conn.close()

def initialize_database():
    global _connection_pool
    with _pool_lock:
        if _connection_pool is not None: return True
        if not create_database_if_not_exists(): return False
        try:
            _connection_pool = pool.ThreadedConnectionPool(
                minconn=2, maxconn=20, host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
            )
            create_tables()
            return True
        except Exception as e:
            logger.exception(f"Database initialization failed: {e}")
            return False

def get_connection():
    if _connection_pool is None: raise RuntimeError("Database pool is not initialized.")
    return _connection_pool.getconn()

def release_connection(conn):
    if conn:
        try: _connection_pool.putconn(conn)
        except Exception: conn.close()

def create_tables():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS stations (
            station_mn VARCHAR(32) PRIMARY KEY,
            station_name VARCHAR(100),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        for mn, info in STATIONS.items():
            cur.execute("""
                INSERT INTO stations (station_mn, station_name, latitude, longitude) 
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (station_mn) DO UPDATE 
                SET station_name = EXCLUDED.station_name, 
                    latitude = EXCLUDED.latitude, 
                    longitude = EXCLUDED.longitude,
                    updated_at = CURRENT_TIMESTAMP;
            """, (mn, info.get("station_name", mn), info.get("latitude"), info.get("longitude")))

        cur.execute("""
        CREATE TABLE IF NOT EXISTS sensor_data (
            id BIGSERIAL PRIMARY KEY,
            station_mn VARCHAR(32) NOT NULL REFERENCES stations(station_mn),
            ip_address INET,
            data_time TIMESTAMP,
            pm25 DOUBLE PRECISION, pm10 DOUBLE PRECISION, tsp DOUBLE PRECISION,
            ozone DOUBLE PRECISION, carbon_monoxide DOUBLE PRECISION,
            sulfur_dioxide DOUBLE PRECISION, nitrogen_dioxide DOUBLE PRECISION,
            temperature DOUBLE PRECISION, humidity DOUBLE PRECISION,
            rain DOUBLE PRECISION, wind_speed DOUBLE PRECISION,
            wind_direction DOUBLE PRECISION, air_pressure DOUBLE PRECISION,
            noise DOUBLE PRECISION, lead DOUBLE PRECISION,
            lead_temperature DOUBLE PRECISION,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sensor_time ON sensor_data(data_time);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_station ON sensor_data(station_mn);")
        conn.commit()
    except Exception as e:
        if conn: conn.rollback()
        logger.exception(f"Table creation failed: {e}")
    finally:
        if conn: release_connection(conn)

def insert_sensor_data(data, ip_address):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cp = data.get("CP", {})
        mn = data.get("MN")
        
        data_time = datetime.now(timezone.utc)

        if "DataTime" in cp:
            try: 
                naive_dt = datetime.strptime(cp["DataTime"], "%Y%m%d%H%M%S")
                philippines_tz = timezone(timedelta(hours=8))
                localized_pht_dt = naive_dt.replace(tzinfo=philippines_tz)
                data_time = localized_pht_dt.astimezone(timezone.utc)
            except ValueError: 
                pass

        values = {"station_mn": mn, "ip_address": ip_address, "data_time": data_time}

        for code, sensor in SENSORS.items():
            if sensor["name"] not in cp: continue
            sensor_data = cp[sensor["name"]]
            values[sensor["column"]] = sensor_data.get("Rtd") or sensor_data.get("Avg") or sensor_data.get("Value")

        columns = list(values.keys())
        placeholders = ["%s"] * len(columns)
        sql = f"INSERT INTO sensor_data ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        
        cur.execute(sql, [values[col] for col in columns])
        conn.commit()
    except Exception as e:
        if conn: conn.rollback()
        logger.error(f"Error inserting sensor data: {e}")
    finally:
        if conn: release_connection(conn)

def update_lead_value(mn, lead, temperature):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE sensor_data SET lead = %s, lead_temperature = %s
            WHERE id = (
                SELECT id FROM sensor_data WHERE station_mn = %s
                ORDER BY data_time DESC LIMIT 1
            )
            """, (lead, temperature, mn))
        conn.commit()
    except Exception as e:
        if conn: conn.rollback()
        logger.error(f"Error updating Modbus lead values: {e}")
    finally:
        if conn: release_connection(conn)


# ==========================================================
# HJ212 PROTOCOL & PARSER
# ==========================================================
def crc16(data: str) -> str:
    crc = 0xFFFF
    for b in data.encode("ascii"):
        crc ^= b
        for _ in range(8):
            if crc & 0x0001:
                crc >>= 1
                crc ^= 0xA001
            else: crc >>= 1
    return f"{crc:04X}"

def verify_crc(frame: str) -> bool:
    try: return frame[-4:].upper() == crc16(frame[6:-4])
    except: return False

def get_field(frame: str, field: str) -> str:
    match = re.search(rf"{field}=([^;]+)", frame)
    return match.group(1) if match else ""

def extract_frames(buffer: str):
    frames = []
    while True:
        start = buffer.find("##")
        if start == -1 or len(buffer) < start + 6: break
        try: length = int(buffer[start + 2:start + 6])
        except ValueError:
            buffer = buffer[start + 2:]
            continue
        total_length = 2 + 4 + length + 4
        if len(buffer) < start + total_length: break
        frames.append(buffer[start:start + total_length])
        buffer = buffer[start + total_length:]
    return frames, buffer

def build_ack(frame: str) -> str:
    body = f"QN={get_field(frame, 'QN')};ST=91;CN=9014;PW={get_field(frame, 'PW')};MN={get_field(frame, 'MN')};Flag=4;CP=&&QnRtn=1;ExeRtn=1&&"
    return f"##{len(body):04d}{body}{crc16(body)}\r\n"

def parse_cp(cp_data: str):
    result = {}
    if not cp_data: return result
    for section in cp_data.split(";"):
        if not section: continue
        if section.startswith("DataTime="):
            result["DataTime"] = section.split("=", 1)[1]
            continue
        for value in section.split(","):
            if "=" not in value: continue
            key, val = value.split("=", 1)
            code, metric = key.split("-", 1) if "-" in key else (key, "Value")
            sensor = SENSOR_MAP.get(code, code)
            if sensor not in result: result[sensor] = {}
            try: result[sensor][metric] = float(val)
            except: pass
    return result

def parse_frame(frame: str):
    frame = frame.replace("\r", "").replace("\n", "")
    if not frame.startswith("##"): return None
    body = frame[6:]
    fields = dict(re.findall(r"(\w+)=([^;]+)", body))
    cp_match = re.search(r"CP=&&(.+?)&&", body)
    return {"Length": frame[2:6], "QN": fields.get("QN"), "CN": fields.get("CN"), "MN": fields.get("MN"), "CP": parse_cp(cp_match.group(1) if cp_match else "")}

def process_frame(frame, ip_address):
    data = parse_frame(frame)
    if data: insert_sensor_data(data, ip_address)


# ==========================================================
# MODBUS LEAD SENSOR SERVICE
# ==========================================================
def poll_station(mn, station):
    ip, port, slave = station["lead_ip"], station["lead_port"], station["lead_slave"]
    client = ModbusTcpClient(host=ip, port=port, framer=FramerType.RTU, timeout=3)
    if not client.connect(): return
    try:
        rr = None
        try:
            rr = client.read_holding_registers(address=0, count=10, slave=slave)
        except TypeError:
            try:
                rr = client.read_holding_registers(address=0, count=10, device_id=slave)
            except TypeError:
                rr = client.read_holding_registers(address=0, count=10, unit=slave)

        if rr and not rr.isError():
            update_lead_value(mn, rr.registers[2] / 10.0, rr.registers[1] / 10.0)
    except Exception as e: 
        logger.error(f"[MODBUS] IP {ip}: {e}")
    finally: 
        client.close()

def lead_service():
    while True:
        for mn, station in STATIONS.items():
            if station["enabled"]: poll_station(mn, station)
        time.sleep(LEAD_POLL_INTERVAL)

def start_lead_service():
    threading.Thread(target=lead_service, daemon=True, name="ModbusLeadService").start()


# ==========================================================
# TCP SERVER
# ==========================================================
def handle_client(conn, addr):
    ip_address = addr[0]
    buffer = ""
    while True:
        try:
            data = conn.recv(BUFFER_SIZE)
            if not data: break
            buffer += data.decode(errors="ignore")
            frames, buffer = extract_frames(buffer)

            for frame in frames:
                if VERIFY_CHECKSUM and not verify_crc(frame): continue
                cn = get_field(frame, "CN")
                if cn == "2011": process_frame(frame, ip_address)
                if cn in SUPPORTED_CN: conn.sendall(build_ack(frame).encode())
        except Exception: break
    conn.close()

def start_tcp_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((SERVER_HOST, SERVER_PORT))
    server.listen(MAX_CONNECTIONS)
    logger.info(f"TCP Server running on {SERVER_HOST}:{SERVER_PORT}")
    while True:
        conn, addr = server.accept()
        conn.settimeout(60)
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


# ==========================================================
# FASTAPI APPLICATION
# ==========================================================
app = FastAPI(
    title="Air Quality & Weather Monitoring System API", 
    version="3.5",
    description="Operational REST endpoints serving live standardized millisecond UTC metrics and historical calculations."
)
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)

def verify_api_key(api_key: str = Security(api_key_header)):
    if api_key != API_TOKEN:
        raise HTTPException(status_code=403, detail="Unauthorized request")
    return api_key

def map_station_row_to_json(row, now):
    """Reusable helper to cleanly convert a query result into the requested station dictionary format."""
    status = "offline"
    last_update_str = None
    if row['data_time']:
        last_update_utc = row['data_time'].replace(tzinfo=timezone.utc)
        last_update_str = format_api_datetime(last_update_utc)
        if (now - last_update_utc).total_seconds() < 900:
            status = "online"

    return {
        "station_mn": row['station_mn'],
        "friendly_name": row['station_name'], 
        "location": {
            "latitude": row['latitude'],
            "longitude": row['longitude']
        },
        "status": status,
        "last_update": last_update_str,
        "weather": {
            "temperature": row['temperature'],
            "humidity": row['humidity'],
            "pressure": row['air_pressure']
        },
        "pollutants": {
            "pm2_5": row['pm25'],
            "pm10": row['pm10'],
            "co": row['carbon_monoxide'],
            "co2": None, 
            "so2": row['sulfur_dioxide'],
            "no2": row['nitrogen_dioxide'],
            "o3": row['ozone'],
            "pb": row['lead'],
            "pb_temp": row['lead_temperature']
        }
    }

# 1. LIVE LATEST: ALL STATIONS
@app.get("/api/v1/stations/latest", tags=["Live Monitoring"])
def get_latest_data(api_key: str = Depends(verify_api_key)):
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        query = """
            SELECT DISTINCT ON (st.station_mn) 
                st.station_mn, st.station_name, st.latitude, st.longitude, s.data_time,
                s.temperature, s.humidity, s.air_pressure, 
                s.pm25, s.pm10, s.carbon_monoxide, s.sulfur_dioxide, 
                s.nitrogen_dioxide, s.ozone, s.lead, s.lead_temperature
            FROM stations st
            LEFT JOIN sensor_data s ON st.station_mn = s.station_mn
            ORDER BY st.station_mn, s.data_time DESC NULLS LAST;
        """
        cur.execute(query)
        rows = cur.fetchall()

        stations_list = []
        now = datetime.now(timezone.utc)
        for row in rows:
            stations_list.append(map_station_row_to_json(row, now))

        return {
            "timestamp": format_api_datetime(now),
            "total_stations": len(stations_list),
            "stations": stations_list
        }
    except Exception as e:
        logger.error(f"API Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        release_connection(conn)

# 2. LIVE LATEST: SPECIFIC STATION (NEW FEATURE #1)
@app.get("/api/v1/stations/{station_mn}/latest", tags=["Live Monitoring"])
def get_specific_station_latest(station_mn: str, api_key: str = Depends(verify_api_key)):
    """Fetches the absolute latest sensor packet recorded for a single specific station MN string."""
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        query = """
            SELECT st.station_mn, st.station_name, st.latitude, st.longitude, s.data_time,
                   s.temperature, s.humidity, s.air_pressure, 
                   s.pm25, s.pm10, s.carbon_monoxide, s.sulfur_dioxide, 
                   s.nitrogen_dioxide, s.ozone, s.lead, s.lead_temperature
            FROM stations st
            LEFT JOIN sensor_data s ON st.station_mn = s.station_mn
            WHERE st.station_mn = %s
            ORDER BY s.data_time DESC NULLS LAST LIMIT 1;
        """
        cur.execute(query, (station_mn,))
        row = cur.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="Station Identifier not found in database records.")
            
        now = datetime.now(timezone.utc)
        return {
            "timestamp": format_api_datetime(now),
            "station": map_station_row_to_json(row, now)
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"API Error fetching single station profile: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        release_connection(conn)

# 3. ANALYTICS: 1 DAY AVERAGE (NEW FEATURE #2)
@app.get("/api/v1/stations/analytics/1d", tags=["Historical Analytics"])
def get_past_day_averages(api_key: str = Depends(verify_api_key)):
    """Returns a single compiled object per station capturing mathematical sensor averages over the trailing 24 hours."""
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        query = """
            SELECT st.station_mn, st.station_name, st.latitude, st.longitude,
                   ROUND(AVG(s.temperature)::numeric, 2) as temperature,
                   ROUND(AVG(s.humidity)::numeric, 2) as humidity,
                   ROUND(AVG(s.air_pressure)::numeric, 2) as air_pressure,
                   ROUND(AVG(s.pm25)::numeric, 2) as pm25,
                   ROUND(AVG(s.pm10)::numeric, 2) as pm10,
                   ROUND(AVG(s.carbon_monoxide)::numeric, 2) as carbon_monoxide,
                   ROUND(AVG(s.sulfur_dioxide)::numeric, 2) as sulfur_dioxide,
                   ROUND(AVG(s.nitrogen_dioxide)::numeric, 2) as nitrogen_dioxide,
                   ROUND(AVG(s.ozone)::numeric, 2) as ozone,
                   ROUND(AVG(s.lead)::numeric, 2) as lead,
                   ROUND(AVG(s.lead_temperature)::numeric, 2) as lead_temperature
            FROM stations st
            JOIN sensor_data s ON st.station_mn = s.station_mn
            WHERE s.data_time >= NOW() - INTERVAL '1 day'
            GROUP BY st.station_mn, st.station_name, st.latitude, st.longitude
            ORDER BY st.station_mn;
        """
        cur.execute(query)
        return {
            "range": "24_hours_aggregated_average",
            "timestamp": format_api_datetime(datetime.now(timezone.utc)),
            "results": cur.fetchall()
        }
    except Exception as e:
        logger.error(f"1-Day Analytics Failure: {e}")
        raise HTTPException(status_code=500, detail="Internal Analytical Server Error")
    finally:
        release_connection(conn)

# 4. ANALYTICS: 7 DAY DAILY TIME-SERIES AVERAGES (NEW FEATURE #3)
@app.get("/api/v1/stations/analytics/7d", tags=["Historical Analytics"])
def get_past_week_daily_averages(api_key: str = Depends(verify_api_key)):
    """Returns a rolling chronological daily time-series array mapping localized sensor step averages across the trailing 7 days."""
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        query = """
            SELECT st.station_mn, st.station_name,
                   DATE_TRUNC('day', s.data_time) as summary_date,
                   ROUND(AVG(s.temperature)::numeric, 2) as temperature,
                   ROUND(AVG(s.humidity)::numeric, 2) as humidity,
                   ROUND(AVG(s.air_pressure)::numeric, 2) as air_pressure,
                   ROUND(AVG(s.pm25)::numeric, 2) as pm25,
                   ROUND(AVG(s.pm10)::numeric, 2) as pm10,
                   ROUND(AVG(s.carbon_monoxide)::numeric, 2) as carbon_monoxide,
                   ROUND(AVG(s.sulfur_dioxide)::numeric, 2) as sulfur_dioxide,
                   ROUND(AVG(s.nitrogen_dioxide)::numeric, 2) as nitrogen_dioxide,
                   ROUND(AVG(s.ozone)::numeric, 2) as ozone,
                   ROUND(AVG(s.lead)::numeric, 2) as lead,
                   ROUND(AVG(s.lead_temperature)::numeric, 2) as lead_temperature
            FROM stations st
            JOIN sensor_data s ON st.station_mn = s.station_mn
            WHERE s.data_time >= NOW() - INTERVAL '7 days'
            GROUP BY st.station_mn, st.station_name, summary_date
            ORDER BY st.station_mn, summary_date DESC;
        """
        cur.execute(query)
        rows = cur.fetchall()
        for r in rows:
            if r['summary_date']:
                r['summary_date'] = r['summary_date'].strftime("%Y-%m-%d")
        return {"range": "7_days_daily_averages", "results": rows}
    except Exception as e:
        logger.error(f"7-Day Analytics Failure: {e}")
        raise HTTPException(status_code=500, detail="Internal Analytical Server Error")
    finally:
        release_connection(conn)

# 5. ANALYTICS: 30 DAY DAILY TIME-SERIES AVERAGES (NEW FEATURE #4)
@app.get("/api/v1/stations/analytics/30d", tags=["Historical Analytics"])
def get_past_month_daily_averages(api_key: str = Depends(verify_api_key)):
    """Returns a complete time-series dashboard payload averaging device sensor values by day over the trailing 30 days."""
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        query = """
            SELECT st.station_mn, st.station_name,
                   DATE_TRUNC('day', s.data_time) as summary_date,
                   ROUND(AVG(s.temperature)::numeric, 2) as temperature,
                   ROUND(AVG(s.humidity)::numeric, 2) as humidity,
                   ROUND(AVG(s.air_pressure)::numeric, 2) as air_pressure,
                   ROUND(AVG(s.pm25)::numeric, 2) as pm25,
                   ROUND(AVG(s.pm10)::numeric, 2) as pm10,
                   ROUND(AVG(s.carbon_monoxide)::numeric, 2) as carbon_monoxide,
                   ROUND(AVG(s.sulfur_dioxide)::numeric, 2) as sulfur_dioxide,
                   ROUND(AVG(s.nitrogen_dioxide)::numeric, 2) as nitrogen_dioxide,
                   ROUND(AVG(s.ozone)::numeric, 2) as ozone,
                   ROUND(AVG(s.lead)::numeric, 2) as lead,
                   ROUND(AVG(s.lead_temperature)::numeric, 2) as lead_temperature
            FROM stations st
            JOIN sensor_data s ON st.station_mn = s.station_mn
            WHERE s.data_time >= NOW() - INTERVAL '30 days'
            GROUP BY st.station_mn, st.station_name, summary_date
            ORDER BY st.station_mn, summary_date DESC;
        """
        cur.execute(query)
        rows = cur.fetchall()
        for r in rows:
            if r['summary_date']:
                r['summary_date'] = r['summary_date'].strftime("%Y-%m-%d")
        return {"range": "30_days_daily_averages", "results": rows}
    except Exception as e:
        logger.error(f"30-Day Analytics Failure: {e}")
        raise HTTPException(status_code=500, detail="Internal Analytical Server Error")
    finally:
        release_connection(conn)

# 6. MANAGEMENT: INDEX ALL REGISTERED STATIONS
@app.get("/api/v1/stations", tags=["Station Infrastructure"])
def list_stations(api_key: str = Depends(verify_api_key)):
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT station_mn, station_name, latitude, longitude, updated_at FROM stations ORDER BY station_mn;")
        rows = cur.fetchall()
        for r in rows:
            if r['updated_at']:
                r['updated_at'] = format_api_datetime(r['updated_at'])
        return {"total_registered": len(rows), "stations": rows}
    except Exception as e:
        logger.error(f"API Stations Fetch Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Database Error")
    finally:
        release_connection(conn)

# 7. DIAGNOSTICS: RUNTIME HEALTH METRICS
@app.get("/api/v1/system/status", tags=["Station Infrastructure"])
def system_health_check():
    pool_available = "Error"
    if _connection_pool:
        pool_available = f"Initialized ({_connection_pool.minconn} min / {_connection_pool.maxconn} max connections)"
    
    return {
        "status": "operational",
        "timestamp": format_api_datetime(datetime.now(timezone.utc)),
        "subsystems": {
            "tcp_ingestion_server": "running",
            "modbus_polling_client": "active",
            "postgresql_pool": pool_available
        }
    }


# ==========================================================
# EXECUTION
# ==========================================================
if __name__ == "__main__":
    if initialize_database():
        start_lead_service()
        
        # Start TCP Server in a background thread so Uvicorn can own the main thread
        threading.Thread(target=start_tcp_server, daemon=True, name="TCP-Main").start()
        
        # Run FastAPI on the main thread
        logger.info(f"API Server starting on 0.0.0.0:{API_PORT}")
        uvicorn.run(app, host="0.0.0.0", port=API_PORT, log_level="warning")
    else:
        logger.critical("Initialization checks incomplete. Halting.")