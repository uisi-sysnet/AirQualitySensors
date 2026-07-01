"""
==========================================================
database.py
HJ212 Air Quality Database Layer
==========================================================

Features
--------
- PostgreSQL Connection Pool
- Automatic Table Creation
- Thread Safe
- Automatic Reconnection
- Lead Sensor Update
- Historical Queries
"""

import logging
import threading
from datetime import datetime

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
)

from sensors import (
    SENSORS,
    get_database_column,
)

# ==========================================================
# Logging
# ==========================================================

logger = logging.getLogger("database")

# ==========================================================
# Connection Pool
# ==========================================================

_connection_pool = None

_pool_lock = threading.Lock()

# ==========================================================
# Create Database if it does not exist
# ==========================================================

def create_database_if_not_exists():
    """
    Creates the application database if it does not already exist.
    """

    conn = None

    try:

        logger.info(f"Checking database '{DB_NAME}'...")

        # Connect to the default postgres database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database="postgres",
            user=DB_USER,
            password=DB_PASSWORD,
        )

        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        cur = conn.cursor()

        # Check if database exists
        cur.execute(
            """
            SELECT 1
            FROM pg_database
            WHERE datname = %s
            """,
            (DB_NAME,)
        )

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

# ==========================================================
# Initialize Connection Pool
# ==========================================================

def initialize_database():
    """
    Initialize the PostgreSQL connection pool.

    Returns
    -------
    bool
        True if successful, False otherwise.
    """
    
    global _connection_pool

    with _pool_lock:

        if _connection_pool is not None:
            logger.info("Database connection pool already initialized.")
            return True

        if not create_database_if_not_exists():
            return False


        try:

            _connection_pool = pool.ThreadedConnectionPool(
                minconn=2,
                maxconn=20,
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
            )

            logger.info("Connected to PostgreSQL.")

            create_tables()

            logger.info("Database initialized successfully.")

            return True

        except Exception as e:

            logger.exception(f"Database initialization failed: {e}")

            return False


# ==========================================================
# Get Connection
# ==========================================================

def get_connection():
    """
    Obtain a database connection from the pool.
    """

    global _connection_pool

    if _connection_pool is None:
        raise RuntimeError("Database pool is not initialized.")

    return _connection_pool.getconn()


# ==========================================================
# Return Connection
# ==========================================================

def release_connection(conn):
    """
    Return a connection to the pool.
    """

    global _connection_pool

    if conn is None:
        return

    try:
        _connection_pool.putconn(conn)

    except Exception:
        conn.close()


# ==========================================================
# Close Connection Pool
# ==========================================================

def close_database():
    """
    Close all pooled connections.
    """

    global _connection_pool

    if _connection_pool:

        logger.info("Closing database connection pool...")

        _connection_pool.closeall()

        _connection_pool = None

# ==========================================================
# Create Tables
# ==========================================================

def create_tables():
    """
    Automatically create all required database tables.
    """

    conn = None

    try:

        conn = get_connection()

        cur = conn.cursor()

        # ==================================================
        # Main Sensor Table
        # ==================================================

        cur.execute("""

        CREATE TABLE IF NOT EXISTS sensor_data (

            id BIGSERIAL PRIMARY KEY,

            station_mn VARCHAR(32) NOT NULL,

            ip_address INET,

            data_time TIMESTAMP,

            --------------------------------------------------
            -- Air Quality
            --------------------------------------------------

            pm25 DOUBLE PRECISION,
            pm10 DOUBLE PRECISION,
            tsp DOUBLE PRECISION,

            ozone DOUBLE PRECISION,
            carbon_monoxide DOUBLE PRECISION,
            sulfur_dioxide DOUBLE PRECISION,
            nitrogen_dioxide DOUBLE PRECISION,

            --------------------------------------------------
            -- Weather
            --------------------------------------------------

            temperature DOUBLE PRECISION,
            humidity DOUBLE PRECISION,

            rain DOUBLE PRECISION,

            wind_speed DOUBLE PRECISION,
            wind_direction DOUBLE PRECISION,

            air_pressure DOUBLE PRECISION,

            --------------------------------------------------
            -- Noise
            --------------------------------------------------

            noise DOUBLE PRECISION,

            --------------------------------------------------
            -- Lead Analyzer
            --------------------------------------------------

            lead DOUBLE PRECISION,
            lead_temperature DOUBLE PRECISION,

            --------------------------------------------------
            -- Metadata
            --------------------------------------------------

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

        );

        """)

        # ==================================================
        # Indexes
        # ==================================================

        cur.execute("""

            CREATE INDEX IF NOT EXISTS idx_sensor_time
            ON sensor_data(data_time);

        """)

        cur.execute("""

            CREATE INDEX IF NOT EXISTS idx_station
            ON sensor_data(station_mn);

        """)

        cur.execute("""

            CREATE INDEX IF NOT EXISTS idx_created
            ON sensor_data(created_at);

        """)

        conn.commit()

        logger.info("Database tables verified.")

    except Exception as e:

        if conn:
            conn.rollback()

        logger.exception(f"Create table failed: {e}")

    finally:

        if conn:
            release_connection(conn)

 # ==========================================================
# Insert HJ212 Sensor Data
# ==========================================================

def insert_sensor_data(data, ip_address):
    """
    Insert one HJ212 frame into the database.

    Parameters
    ----------
    data : dict
        Parsed frame from parser.py

    ip_address : str
        Device IP address
    """

    conn = None

    try:

        conn = get_connection()

        cur = conn.cursor()

        cp = data.get("CP", {})

        mn = data.get("MN")



        # ----------------------------------------------
        # Parse DataTime
        # ----------------------------------------------

        data_time = None

        if "DataTime" in cp:

            try:
                data_time = datetime.strptime(
                    cp["DataTime"],
                    "%Y%m%d%H%M%S"
                )
            except ValueError:
                logger.warning("Invalid DataTime received.")

        # ----------------------------------------------
        # Build database values
        # ----------------------------------------------
        
        values = {
            "station_mn": mn,
            "ip_address": ip_address,
            "data_time": data_time,
        }

        # ----------------------------------------------
        # Loop through every sensor
        # ----------------------------------------------

        for code, sensor in SENSORS.items():

            sensor_name = sensor["name"]
            column = sensor["column"]

            if sensor_name not in cp:
                continue

            sensor_data = cp[sensor_name]

            value = sensor_data.get("Rtd")

            if value is None:
                value = sensor_data.get("Avg")

            if value is None:
                value = sensor_data.get("Value")

            values[column] = value

        # ----------------------------------------------
        # Build SQL dynamically
        # ----------------------------------------------

        columns = list(values.keys())

        placeholders = ["%s"] * len(columns)

        sql = f"""
            INSERT INTO sensor_data
            ({', '.join(columns)})
            VALUES
            ({', '.join(placeholders)})
        """

        cur.execute(
            sql,
            [values[col] for col in columns]
        )

        conn.commit()

        logger.info(
            f"Stored data from {mn} ({ip_address})"
        )

    except Exception as e:

        if conn:
            conn.rollback()

        logger.exception(f"Insert failed: {e}")

    finally:

        if conn:
            release_connection(conn)

# ==========================================================
# Update Latest Lead Value
# ==========================================================

def update_lead_value(mn, lead, temperature):
    """
    Update the latest record for a station with Lead sensor data.
    """

    conn = None

    try:

        conn = get_connection()

        cur = conn.cursor()

        cur.execute(
            """
            UPDATE sensor_data
            SET
                lead = %s,
                lead_temperature = %s
            WHERE id = (
                SELECT id
                FROM sensor_data
                WHERE station_mn = %s
                ORDER BY data_time DESC NULLS LAST,
                         created_at DESC
                LIMIT 1
            )
            """,
            (
                lead,
                temperature,
                mn,
            )
        )

        conn.commit()

        logger.info(f"Lead updated for {mn}")

    except Exception as e:

        if conn:
            conn.rollback()

        logger.exception(f"Lead update failed: {e}")

    finally:

        if conn:
            release_connection(conn)

# ==========================================================
# Get Latest Data
# ==========================================================

def get_latest_data(mn=None):
    """
    Return the latest sensor record.

    Parameters
    ----------
    mn : str, optional
        Station MN number.
        If None, returns the latest record from all stations.
    """

    conn = None

    try:

        conn = get_connection()

        cur = conn.cursor(cursor_factory=RealDictCursor)

        if mn:

            cur.execute(
                """
                SELECT *
                FROM sensor_data
                WHERE station_mn = %s
                ORDER BY data_time DESC NULLS LAST,
                         created_at DESC
                LIMIT 1
                """,
                (mn,)
            )

        else:

            cur.execute(
                """
                SELECT *
                FROM sensor_data
                ORDER BY data_time DESC NULLS LAST,
                         created_at DESC
                LIMIT 1
                """
            )

        return cur.fetchone()

    except Exception as e:

        logger.exception(f"Failed to get latest data: {e}")

        return None

    finally:

        if conn:
            release_connection(conn)

# ==========================================================
# Get Station History
# ==========================================================

def get_station_history(
    mn,
    start_time=None,
    end_time=None,
    limit=1000
):
    """
    Return historical records for one station.
    """

    conn = None

    try:

        conn = get_connection()

        cur = conn.cursor(cursor_factory=RealDictCursor)

        sql = """
            SELECT *
            FROM sensor_data
            WHERE station_mn = %s
        """

        params = [mn]

        if start_time:

            sql += " AND data_time >= %s"
            params.append(start_time)

        if end_time:

            sql += " AND data_time <= %s"
            params.append(end_time)

        sql += """
            ORDER BY data_time DESC
            LIMIT %s
        """

        params.append(limit)

        cur.execute(sql, tuple(params))

        return cur.fetchall()

    except Exception as e:

        logger.exception(f"History query failed: {e}")

        return []

    finally:

        if conn:
            release_connection(conn)

# ==========================================================
# Cleanup Old Records
# ==========================================================

def cleanup_old_records(days=365):
    """
    Delete records older than the specified number of days.

    Parameters
    ----------
    days : int
        Number of days to keep.
    """

    conn = None

    try:

        conn = get_connection()

        cur = conn.cursor()

        cur.execute(
            """
            DELETE FROM sensor_data
            WHERE created_at < NOW() - (%s * INTERVAL '1 day')
            """,
            (days,)
        )

        deleted = cur.rowcount

        conn.commit()

        logger.info(f"Deleted {deleted} old records.")

        return deleted

    except Exception as e:

        if conn:
            conn.rollback()

        logger.exception(f"Cleanup failed: {e}")

        return 0

    finally:

        if conn:
            release_connection(conn)


# ==========================================================
# Execute Custom Query
# ==========================================================

def execute_query(sql, params=None, fetch=False):
    """
    Execute a custom SQL query.

    Parameters
    ----------
    sql : str
        SQL statement.
    params : tuple
        Query parameters.
    fetch : bool
        Return fetched rows if True.
    """

    conn = None

    try:

        conn = get_connection()

        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute(sql, params)

        if fetch:
            result = cur.fetchall()
        else:
            result = None

        conn.commit()

        return result

    except Exception as e:

        if conn:
            conn.rollback()

        logger.exception(f"SQL execution failed: {e}")

        return None

    finally:

        if conn:
            release_connection(conn)


# ==========================================================
# Database Status
# ==========================================================

def database_status():
    """
    Check database connectivity.
    """

    conn = None

    try:

        conn = get_connection()

        cur = conn.cursor()

        cur.execute("SELECT NOW();")

        server_time = cur.fetchone()[0]

        return {
            "connected": True,
            "server_time": server_time,
        }

    except Exception as e:

        logger.exception(f"Database status failed: {e}")

        return {
            "connected": False,
            "server_time": None,
        }

    finally:

        if conn:
            release_connection(conn)   