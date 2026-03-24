"""
pipeline/run_pipeline.py
------------------------
Orchestrates the full multi-hop enrichment pipeline on a 5-minute schedule.

Hop 1: OpenSky   → live flights over North America
Hop 2: OWM       → weather at each flight's position
Hop 3: Lufthansa → airline name from callsign
DB:              → store enriched records in PostgreSQL

Run:
    python pipeline/run_pipeline.py
"""

import os
import sys
import time
import logging
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Allow imports from sibling modules
sys.path.insert(0, os.path.dirname(__file__))

from hop1_opensky   import fetch_flights
from hop2_weather   import enrich_with_weather
from hop3_lufthansa import enrich_with_lufthansa
from db             import insert_flights

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

# Max flights to process per run (free OWM tier: 60 calls/min)
MAX_FLIGHTS = 200


def run_once():
    """Execute one full pipeline run."""
    start = datetime.now()
    print()
    print("=" * 60)
    print(f"  Pipeline run starting at {start.strftime('%H:%M:%S')}")
    print("=" * 60)

    # Hop 1 — live flights
    flights = fetch_flights(limit=MAX_FLIGHTS)
    if not flights:
        print("  No flights returned. Skipping this run.")
        return

    # Hop 2 — weather enrichment
    flights = enrich_with_weather(flights)

    # Hop 3 — Lufthansa airline enrichment
    flights = enrich_with_lufthansa(flights)

    # Store in PostgreSQL
    inserted = insert_flights(flights)
    elapsed = (datetime.now() - start).total_seconds()

    print(f"\n  ✓ {inserted} records inserted in {elapsed:.1f}s")
    print(f"  Next run in 5 minutes...")


def main():
    print("\n🛫  Flight Enrichment Pipeline")
    print("     North America | Every 5 minutes")
    print("     Press Ctrl+C to stop\n")

    # Run immediately on startup
    run_once()

    # Then schedule every 5 minutes
    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_once,
        trigger=IntervalTrigger(minutes=5),
        id='flight_pipeline',
        name='Multi-hop flight enrichment',
        max_instances=1,        # prevent overlapping runs
        coalesce=True,          # skip missed runs if behind
    )

    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("\n  Pipeline stopped.")
        scheduler.shutdown()


if __name__ == '__main__':
    main()
