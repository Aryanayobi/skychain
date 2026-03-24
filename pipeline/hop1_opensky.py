"""
hop1_opensky.py
---------------
Hop 1: Fetch live flights over North America from the OpenSky Network API.

OpenSky is free and requires no API key for anonymous access (rate limited).
North America bounding box: lat 24-72, lon -168 to -52
"""

import requests
from typing import Optional

# North America bounding box
# Global bounding box
BBOX = {
    'lamin': -90.0,
    'lamax': 90.0,
    'lomin': -180.0,
    'lomax': 180.0,
}
OPENSKY_URL = 'https://opensky-network.org/api/states/all'

# Column indices in the OpenSky states array
COL = {
    'icao24':        0,
    'callsign':      1,
    'origin_country':2,
    'time_position': 3,
    'last_contact':  4,
    'longitude':     5,
    'latitude':      6,
    'baro_altitude': 7,
    'on_ground':     8,
    'velocity':      9,
    'true_track':   10,
    'vertical_rate':11,
    'sensors':      12,
    'geo_altitude': 13,
    'squawk':       14,
    'spi':          15,
    'position_src': 16,
}


def fetch_flights(limit: int = 100) -> list[dict]:
    """
    Fetch live flights over North America.
    Returns a list of flight dicts, capped at `limit`.
    """
    try:
        response = requests.get(
            OPENSKY_URL,
            params=BBOX,
            timeout=15
        )
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"  [Hop 1] OpenSky request failed: {e}")
        return []

    data = response.json()
    states = data.get('states', []) or []

    flights = []
    for state in states[:limit]:
        # Skip aircraft with no position
        if state[COL['latitude']] is None or state[COL['longitude']] is None:
            continue
        # Skip aircraft on the ground
        if state[COL['on_ground']]:
            continue

        alt_m = state[COL['baro_altitude']]
        vel_ms = state[COL['velocity']]

        flights.append({
            'icao24':         (state[COL['icao24']] or '').strip(),
            'callsign':       (state[COL['callsign']] or '').strip(),
            'origin_country': state[COL['origin_country']],
            'latitude':       state[COL['latitude']],
            'longitude':      state[COL['longitude']],
            'altitude_m':     alt_m,
            'altitude_ft':    round(alt_m * 3.28084, 2) if alt_m else None,
            'velocity_ms':    vel_ms,
            'velocity_kmh':   round(vel_ms * 3.6, 2) if vel_ms else None,
            'heading':        state[COL['true_track']],
            'vertical_rate':  state[COL['vertical_rate']],
            'on_ground':      state[COL['on_ground']],
        })

    print(f"  [Hop 1] {len(flights)} airborne flights fetched globally")
    return flights
