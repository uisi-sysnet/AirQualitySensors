"""
==========================================================
HJ212 SERVER CONFIGURATION
==========================================================
Modify these values to match your environment.
"""


# ==========================================================
# TCP SERVER
# ==========================================================
SERVER_HOST = "0.0.0.0"
SERVER_PORT = 1935
BUFFER_SIZE = 4096
MAX_CONNECTIONS = 20

# ==========================================================
# POSTGRESQL
# ==========================================================
DB_HOST = "127.0.0.1"
DB_PORT = 5432
DB_NAME = "air_quality"
DB_USER = "postgres"
DB_PASSWORD = "your_password"

# ==========================================================
# LOGGING
# ==========================================================
LOG_LEVEL = "INFO"
LOG_TO_FILE = True
LOG_DIRECTORY = "logs"

# ==========================================================
# DEVICE SETTINGS
# ==========================================================
AUTO_ACK = True
VERIFY_CHECKSUM = False     # Enable later after CRC implementation
ALLOW_UNKNOWN_DEVICES = False

# ==========================================================
# TIMEZONE
# ==========================================================
TIMEZONE = "Asia/Manila"

# ==========================================================
# APPLICATION
# ==========================================================
APP_NAME = "HJ212 Server"
APP_VERSION = "1.0.0"

# ==========================================================
# SUPPORTED COMMANDS
# ==========================================================
SUPPORTED_CN = [
    "2011",   # Real-time Data
    "2031",   # Daily Data
    "2051",   # Hourly Data
    "2061",   # Minute/Period Statistics
    "3020",   # Device Status
    "1012",   # System Command
    "9011",   # ACK Request
    "9012",   # Execution ACK
    "9014",   # Data ACK
]