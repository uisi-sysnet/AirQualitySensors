from config import STATIONS


def get_station(mn):
    """
    Return station information from the registry.
    """
    return STATIONS.get(mn)


def get_lead_ip(mn):
    station = STATIONS.get(mn)

    if station:
        return station["lead_ip"]

    return None


def get_slave(mn):
    station = STATIONS.get(mn)

    if station:
        return station["lead_slave"]

    return 1