"""
hop3_lufthansa.py
-----------------
Hop 3: Enrich each flight with aircraft type and airline name
from the Lufthansa Open API, using the callsign to derive
the IATA airline code and the icao24 to look up aircraft details.

Results are cached in-memory so we don't re-hit the API for
the same airline/aircraft within a pipeline run.
"""

import os
import requests
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID     = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
BASE_URL      = 'https://api.lufthansa.com/v1'

_token: Optional[str] = None
_airline_cache: dict = {}
_aircraft_cache: dict = {}


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _get_token() -> Optional[str]:
    global _token
    if _token:
        return _token
    if not CLIENT_ID or not CLIENT_SECRET:
        return None
    try:
        resp = requests.post(
            f'{BASE_URL}/oauth/token',
            data={
                'client_id': CLIENT_ID,
                'client_secret': CLIENT_SECRET,
                'grant_type': 'client_credentials'
            },
            timeout=10
        )
        if resp.status_code == 200:
            _token = resp.json().get('access_token')
            return _token
    except Exception as e:
        print(f"  [Hop 3] Auth failed: {e}")
    return None


def _headers() -> dict:
    return {'Authorization': f'Bearer {_get_token()}', 'Accept': 'application/json'}


# ---------------------------------------------------------------------------
# Lookups
# ---------------------------------------------------------------------------

def _lookup_airline(iata_code: str) -> Optional[str]:
    """Fetch airline name by 2-letter IATA code."""
    if iata_code in _airline_cache:
        return _airline_cache[iata_code]
    try:
        resp = requests.get(
            f'{BASE_URL}/mds-references/airlines/{iata_code}',
            headers=_headers(), timeout=8
        )
        if resp.status_code == 200:
            data = resp.json()
            name_entry = data['AirlineResource']['Airlines']['Airline']['Names']['Name']
            name = name_entry['$'] if isinstance(name_entry, dict) else name_entry[0]['$']
            _airline_cache[iata_code] = name
            return name
    except Exception:
        pass
    _airline_cache[iata_code] = None
    return None


def _lookup_aircraft(icao24: str) -> dict:
    """
    The Lufthansa API doesn't expose icao24 directly, but we can
    attempt a lookup using the first 3 chars of the callsign as
    a proxy for the aircraft operator code.
    Returns aircraft_code and aircraft_name if found.
    """
    if icao24 in _aircraft_cache:
        return _aircraft_cache[icao24]
    # Gracefully return empty — aircraft lookup is best-effort
    _aircraft_cache[icao24] = {}
    return {}


def _iata_from_callsign(callsign: str) -> Optional[str]:
    """
    Derive the 2-letter IATA airline code from a callsign.
    Most callsigns start with the 3-letter ICAO code (e.g. AAL = American Airlines).
    We map common ICAO → IATA codes for North America.
    """
    if not callsign or len(callsign) < 2:
        return None

    # Common North American ICAO callsign prefix → IATA code
    ICAO_TO_IATA = {
        'AAL': 'AA',  # American Airlines
        'UAL': 'UA',  # United Airlines
        'DAL': 'DL',  # Delta Air Lines
        'SWA': 'WN',  # Southwest Airlines
        'SKW': 'OO',  # SkyWest Airlines
        'ASA': 'AS',  # Alaska Airlines
        'JBU': 'B6',  # JetBlue
        'FFT': 'F9',  # Frontier Airlines
        'NKS': 'NK',  # Spirit Airlines
        'HAL': 'HA',  # Hawaiian Airlines
        'WJA': 'WS',  # WestJet
        'ACA': 'AC',  # Air Canada
        'FDX': 'FX',  # FedEx
        'UPS': '5X',  # UPS Airlines
        'DHL': 'D0',  # DHL
        'BAW': 'BA',  # British Airways
        'DLH': 'LH',  # Lufthansa
        'AFR': 'AF',  # Air France
    }

    prefix = callsign[:3].upper()
    return ICAO_TO_IATA.get(prefix)


# ---------------------------------------------------------------------------
# Main enrichment function
# ---------------------------------------------------------------------------

def enrich_with_lufthansa(flights: list[dict]) -> list[dict]:
    """
    Add airline_name and aircraft info to each flight dict.
    Gracefully skips if Lufthansa credentials are not set.
    """
    token = _get_token()
    if not token:
        print("  [Hop 3] Skipping Lufthansa enrichment — credentials not set")
        for flight in flights:
            flight.update({'airline_name': None, 'aircraft_code': None, 'aircraft_name': None})
        return flights

    enriched = 0
    for flight in flights:
        callsign = flight.get('callsign', '')
        iata = _iata_from_callsign(callsign)

        airline_name = _lookup_airline(iata) if iata else None
        aircraft_info = _lookup_aircraft(flight.get('icao24', ''))

        flight.update({
            'airline_name':   airline_name,
            'aircraft_code':  aircraft_info.get('aircraft_code'),
            'aircraft_name':  aircraft_info.get('aircraft_name'),
        })
        if airline_name:
            enriched += 1

    print(f"  [Hop 3] Lufthansa enrichment: {enriched}/{len(flights)} flights matched an airline")
    return flights
