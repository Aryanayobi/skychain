"""
db.py
-----
Handles PostgreSQL connection and insertion of enriched flight records.
"""

import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DB_HOST     = os.getenv('DB_HOST', 'localhost')
DB_PORT     = os.getenv('DB_PORT', '5432')
DB_NAME     = os.getenv('DB_NAME', 'flightdb')
DB_USER     = os.getenv('DB_USER', 'user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')


def connect():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )


INSERT_SQL = """
    INSERT INTO enriched_flights (
        icao24, callsign, origin_country,
        latitude, longitude, altitude_m, altitude_ft,
        velocity_ms, velocity_kmh, heading, vertical_rate, on_ground,
        nearest_airport,
        origin_weather_temp, origin_weather_cond,
        origin_weather_wind, origin_weather_humid,
        aircraft_code, aircraft_name, airline_name
    ) VALUES (
        %(icao24)s, %(callsign)s, %(origin_country)s,
        %(latitude)s, %(longitude)s, %(altitude_m)s, %(altitude_ft)s,
        %(velocity_ms)s, %(velocity_kmh)s, %(heading)s, %(vertical_rate)s, %(on_ground)s,
        %(nearest_airport)s,
        %(origin_weather_temp)s, %(origin_weather_cond)s,
        %(origin_weather_wind)s, %(origin_weather_humid)s,
        %(aircraft_code)s, %(aircraft_name)s, %(airline_name)s
    )
"""

# Fields that must exist in every record (with defaults)
DEFAULTS = {
    'icao24': None, 'callsign': None, 'origin_country': None,
    'latitude': None, 'longitude': None, 'altitude_m': None, 'altitude_ft': None,
    'velocity_ms': None, 'velocity_kmh': None, 'heading': None,
    'vertical_rate': None, 'on_ground': False,
    'nearest_airport': None,
    'origin_weather_temp': None, 'origin_weather_cond': None,
    'origin_weather_wind': None, 'origin_weather_humid': None,
    'aircraft_code': None, 'aircraft_name': None, 'airline_name': None,
}


def insert_flights(flights: list[dict]) -> int:
    """Insert enriched flight records. Returns number of rows inserted."""
    if not flights:
        return 0

    # Merge defaults so every record has all keys
    records = [{**DEFAULTS, **f} for f in flights]

    conn = None
    try:
        conn = connect()
        psycopg2.extras.execute_batch(conn.cursor(), INSERT_SQL, records, page_size=100)
        conn.commit()
        return len(records)
    except psycopg2.Error as e:
        print(f"  [DB] Insert error: {e.pgcode} - {e.pgerror}")
        return 0
    finally:
        if conn:
            conn.close()
