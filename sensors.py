"""
==========================================================
HJ212 Sensor Definitions
==========================================================
"""

SENSORS = {
    "a34004": {
        "name": "PM2.5",
        "column": "pm25",
        "unit": "µg/m³",
    },

    "a34002": {
        "name": "PM10",
        "column": "pm10",
        "unit": "µg/m³",
    },

    "a34001": {
        "name": "TSP",
        "column": "tsp",
        "unit": "µg/m³",
    },

    "a05024": {
        "name": "Ozone",
        "column": "ozone",
        "unit": "µg/m³",
    },

    "a21005": {
        "name": "Carbon Monoxide",
        "column": "carbon_monoxide",
        "unit": "mg/m³",
    },

    "a21026": {
        "name": "Sulfur Dioxide",
        "column": "sulfur_dioxide",
        "unit": "µg/m³",
    },

    "a21004": {
        "name": "Nitrogen Dioxide",
        "column": "nitrogen_dioxide",
        "unit": "µg/m³",
    },

    "a01001": {
        "name": "Temperature",
        "column": "temperature",
        "unit": "°C",
    },

    "a01002": {
        "name": "Humidity",
        "column": "humidity",
        "unit": "%",
    },

    "a06001": {
        "name": "Rain",
        "column": "rain",
        "unit": "mm",
    },

    "LA": {
        "name": "Noise",
        "column": "noise",
        "unit": "dB",
    },

    "a01007": {
        "name": "Wind Speed",
        "column": "wind_speed",
        "unit": "m/s",
    },

    "a01008": {
        "name": "Wind Direction",
        "column": "wind_direction",
        "unit": "°",
    },

    "a01006": {
        "name": "Air Pressure",
        "column": "air_pressure",
        "unit": "kPa",
    },
}


# ==========================================================
# Backward-compatible sensor name lookup
# ==========================================================

SENSOR_MAP = {
    code: sensor["name"]
    for code, sensor in SENSORS.items()
}


# ==========================================================
# Utility Functions
# ==========================================================

def get_sensor(code):
    """
    Returns the sensor definition.

    Example:
        get_sensor("a34004")
    """

    return SENSORS.get(code)


def get_sensor_name(code):
    """
    Returns the display name.

    Example:
        PM2.5
    """

    sensor = get_sensor(code)

    if sensor:
        return sensor["name"]

    return code


def get_database_column(code):
    """
    Returns the PostgreSQL column name.

    Example:
        pm25
    """

    sensor = get_sensor(code)

    if sensor:
        return sensor["column"]

    return None


def get_unit(code):
    """
    Returns the engineering unit.

    Example:
        µg/m³
    """

    sensor = get_sensor(code)

    if sensor:
        return sensor["unit"]

    return ""