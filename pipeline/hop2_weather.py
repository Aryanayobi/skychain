"""
hop2_weather.py
---------------
Hop 2: Enrich each flight with weather at its current position.

Uses OpenWeatherMap's "weather by coordinates" endpoint.
To avoid hammering the API, we cache results per ~1-degree grid cell
so nearby flights share the same weather lookup.
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

OWM_API_KEY  = os.getenv('OWM_API_KEY')
OWM_URL      = 'https://api.openweathermap.org/data/2.5/weather'

# Simple in-memory cache: grid_key → weather dict
# Rounds lat/lon to 1 decimal degree (~11km grid) to share lookups
_cache: dict = {}


def _grid_key(lat: float, lon: float) -> tuple:
    return (round(lat, 1), round(lon, 1))


def _kelvin_to_celsius(k: float) -> float:
    return round(k - 273.15, 2)


def _fetch_weather(lat: float, lon: float) -> dict:
    """Raw API call to OWM by coordinates."""
    try:
        resp = requests.get(
            OWM_URL,
            params={'lat': lat, 'lon': lon, 'appid': OWM_API_KEY},
            timeout=10
        )
        if resp.status_code != 200:
            return {}
        data = resp.json()
        return {
            'nearest_airport':      data.get('name', ''),
            'origin_weather_temp':  _kelvin_to_celsius(data['main']['temp']),
            'origin_weather_cond':  data['weather'][0]['main'],
            'origin_weather_wind':  data['wind']['speed'],
            'origin_weather_humid': data['main']['humidity'],
        }
    except Exception as e:
        print(f"  [Hop 2] Weather fetch failed ({lat},{lon}): {e}")
        return {}


def enrich_with_weather(flights: list[dict]) -> list[dict]:
    """
    Add weather fields to each flight dict.
    Uses grid-level caching to minimise API calls.
    """
    api_calls = 0
    for flight in flights:
        lat = flight.get('latitude')
        lon = flight.get('longitude')
        if lat is None or lon is None:
            continue

        key = _grid_key(lat, lon)
        if key not in _cache:
            _cache[key] = _fetch_weather(lat, lon)
            api_calls += 1

        flight.update(_cache[key])

    print(f"  [Hop 2] Weather enriched {len(flights)} flights "
          f"({api_calls} API calls, {len(flights) - api_calls} cache hits)")
    return flights
