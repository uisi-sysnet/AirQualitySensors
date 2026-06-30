import psycopg2
from psycopg2.extras import RealDictCursor
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from sensors import get_database_column
from datetime import datetime


# ==========================================================
# CONNECTION
# ==========================================================
def get_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn

    except Exception as e:
        print("\n[DB WARNING] Cannot connect to PostgreSQL")
        print("[DB WARNING]", e)
        return None


# ==========================================================
# GET DEVICE ID
# ==========================================================
def get_device_id(cursor, mn):
    cursor.execute(
        "SELECT id FROM devices WHERE mn = %s",
        (mn,)
    )
    row = cursor.fetchone()
    return row[0] if row else None


# ==========================================================
# INSERT SENSOR DATA (ONLY ONE VERSION)
# ==========================================================
def insert_sensor_data(parsed_frame, ip_address=None):

    conn = get_connection()

    if not conn:
        print("[DB WARNING] No DB connection, skipping insert")
        return

    cursor = conn.cursor()

    try:
        # ----------------------------
        # DATA EXTRACTION
        # ----------------------------
        mn = parsed_frame.get("MN")
        cp = parsed_frame.get("CP", {})
        data_time_raw = cp.get("DataTime")

        data_time = None
        if data_time_raw:
            data_time = datetime.strptime(data_time_raw, "%Y%m%d%H%M%S")

        # ----------------------------
        # DEVICE LOOKUP
        # ----------------------------
        device_id = get_device_id(cursor, mn)

        if not device_id:
            print(f"[DB] Unknown device MN: {mn}")
            return

        # ----------------------------
        # BUILD INSERT
        # ----------------------------
        columns = ["device_id", "ip_address", "data_time"]
        values = [device_id, ip_address, data_time]
        placeholders = ["%s", "%s", "%s"]

        for code, sensor_data in cp.items():

            if code == "DataTime":
                continue

            column = get_database_column(code)

            if not column:
                continue

            value = sensor_data.get("Rtd") or sensor_data.get("Avg")

            if value is None:
                continue

            columns.append(column)
            values.append(value)
            placeholders.append("%s")

        sql = f"""
            INSERT INTO aq_sensors (
                {",".join(columns)}
            )
            VALUES (
                {",".join(placeholders)}
            )
        """

        cursor.execute(sql, values)
        conn.commit()

        print(f"[DB] Inserted data | MN={mn} | IP={ip_address}")

    except Exception as e:
        conn.rollback()
        print(f"[DB ERROR] Insert failed but server continues: {e}")

    finally:
        cursor.close()
        conn.close()