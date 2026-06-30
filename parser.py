import re

from sensors import SENSOR_MAP
from database import insert_sensor_data


# ==========================================================
# Parse CP Section
# ==========================================================
def parse_cp(cp_data: str):

    result = {}

    if not cp_data:
        return result

    sections = cp_data.split(";")

    for section in sections:

        if not section:
            continue

        # DataTime
        if section.startswith("DataTime="):
            result["DataTime"] = section.split("=", 1)[1]
            continue

        values = section.split(",")

        for value in values:

            if "=" not in value:
                continue

            key, val = value.split("=", 1)

            if "-" in key:
                code, metric = key.split("-", 1)
            else:
                code = key
                metric = "Value"

            sensor = SENSOR_MAP.get(code, code)

            if sensor not in result:
                result[sensor] = {}

            try:
                val = float(val)
            except:
                pass

            result[sensor][metric] = val

    return result


# ==========================================================
# Parse Full Frame
# ==========================================================
def parse_frame(frame: str):

    frame = frame.replace("\r", "").replace("\n", "")

    if not frame.startswith("##"):
        return None

    length = frame[2:6]
    body = frame[6:]

    fields = dict(re.findall(r"(\w+)=([^;]+)", body))

    cp_match = re.search(r"CP=&&(.+?)&&", body)
    cp_data = cp_match.group(1) if cp_match else ""

    return {
        "Length": length,
        "QN": fields.get("QN"),
        "ST": fields.get("ST"),
        "CN": fields.get("CN"),
        "PW": fields.get("PW"),
        "MN": fields.get("MN"),
        "Flag": fields.get("Flag"),
        "CP": parse_cp(cp_data),
        "RawCP": cp_data
    }


# ==========================================================
# Pretty Print (Console Debug)
# ==========================================================
def print_frame(data):

    if not data:
        return

    print("\n" + "=" * 60)
    print("HJ212 FRAME")
    print("=" * 60)

    print(f"Length : {data['Length']}")
    print(f"QN     : {data['QN']}")
    print(f"ST     : {data['ST']}")
    print(f"CN     : {data['CN']}")
    print(f"MN     : {data['MN']}")

    cp = data["CP"]

    if "DataTime" in cp:
        print(f"DataTime : {cp['DataTime']}")

    print("\nSensor                     Value")
    print("-" * 45)

    for sensor, values in cp.items():

        if sensor == "DataTime":
            continue

        value = (
            values.get("Rtd")
            or values.get("Avg")
            or values.get("Value")
        )

        print(f"{sensor:25} {value}")


# ==========================================================
# MAIN PIPELINE
# ==========================================================
def process_frame(frame, ip_address):

    data = parse_frame(frame)

    if not data:
        return

    print_frame(data)

    # IMPORTANT: send correct variables
    insert_sensor_data(data, ip_address)