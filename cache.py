"""
==========================================================
Shared Cache
Air Quality Monitoring Server v2.0
==========================================================

This module stores the latest Lead Sensor readings
for each HJ212 station (MN).

The cache is thread-safe.

Used by:

    lead_sensor.py
    database.py
"""

import threading
from datetime import datetime


# ==========================================================
# THREAD LOCK
# ==========================================================

_cache_lock = threading.Lock()


# ==========================================================
# CACHE
# ==========================================================

_lead_cache = {}


# ==========================================================
# UPDATE LEAD VALUE
# ==========================================================

def update_lead(mn, lead_ppm, temperature):

    with _cache_lock:

        _lead_cache[mn] = {

            "lead_ppm": float(lead_ppm),

            "lead_temperature": float(temperature),

            "updated_at": datetime.now()

        }


# ==========================================================
# GET LEAD VALUE
# ==========================================================

def get_lead(mn):

    with _cache_lock:

        return _lead_cache.get(mn)


# ==========================================================
# REMOVE DEVICE
# ==========================================================

def remove_device(mn):

    with _cache_lock:

        if mn in _lead_cache:

            del _lead_cache[mn]


# ==========================================================
# CLEAR CACHE
# ==========================================================

def clear():

    with _cache_lock:

        _lead_cache.clear()


# ==========================================================
# GET ENTIRE CACHE
# ==========================================================

def get_all():

    with _cache_lock:

        return dict(_lead_cache)


# ==========================================================
# CACHE SIZE
# ==========================================================

def size():

    with _cache_lock:

        return len(_lead_cache)


# ==========================================================
# PRINT CACHE
# ==========================================================

def print_cache():

    with _cache_lock:

        print()

        print("=" * 60)

        print("Lead Sensor Cache")

        print("=" * 60)

        if not _lead_cache:

            print("Empty")

        else:

            for mn, value in _lead_cache.items():

                print(

                    f"{mn} "

                    f"{value['lead_ppm']} ppm "

                    f"{value['lead_temperature']} °C"

                )

        print("=" * 60)