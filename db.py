"""
==========================================================
PostgreSQL Connection Manager
Air Quality Server v2.0
==========================================================
"""

from psycopg2.pool import ThreadedConnectionPool

from config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
)


# ==========================================================
# CONNECTION POOL
# ==========================================================

_pool = None


# ==========================================================
# INITIALIZE
# ==========================================================

def initialize():

    global _pool

    if _pool is not None:
        return

    print("[DB] Creating PostgreSQL Connection Pool...")

    _pool = ThreadedConnectionPool(

        minconn=1,

        maxconn=20,

        host=DB_HOST,

        port=DB_PORT,

        database=DB_NAME,

        user=DB_USER,

        password=DB_PASSWORD,

    )

    print("[DB] Connection Pool Ready")


# ==========================================================
# GET CONNECTION
# ==========================================================

def get_connection():

    if _pool is None:

        initialize()

    return _pool.getconn()


# ==========================================================
# RETURN CONNECTION
# ==========================================================

def return_connection(conn):

    if conn is None:
        return

    _pool.putconn(conn)


# ==========================================================
# CLOSE ALL CONNECTIONS
# ==========================================================

def close():

    global _pool

    if _pool is None:
        return

    print("[DB] Closing Connection Pool")

    _pool.closeall()

    _pool = None