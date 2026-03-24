# ✈️ Multi-Hop Flight Enrichment Pipeline

A data engineering pipeline that fetches live flight data over North America and progressively enriches each record across three API hops — adding weather conditions, airline identity, and aircraft details — before storing the fully enriched records in PostgreSQL and visualizing them on a live Dash dashboard.

---

## Architecture

```
Every 5 minutes (APScheduler)
          ↓
Hop 1:  OpenSky Network API   →  live flights over North America
          ↓                       (position, speed, altitude, heading)
Hop 2:  OpenWeatherMap API    →  weather at each flight's position
          ↓                       (temp, condition, wind, humidity)
Hop 3:  Lufthansa Open API    →  airline name from callsign
          ↓                       (airline_name, aircraft details)
        PostgreSQL            →  enriched_flights table
          ↓
        Dash Dashboard        →  live map + charts, auto-refreshes
```

Each flight record starts as 10 raw fields from OpenSky and ends up as a fully enriched record with 20+ fields from three independent data sources.

---

## Project Structure

```
flight-enrichment/
├── pipeline/
│   ├── hop1_opensky.py        # Fetch live flights (OpenSky)
│   ├── hop2_weather.py        # Enrich with weather (OpenWeatherMap)
│   ├── hop3_lufthansa.py      # Enrich with airline info (Lufthansa API)
│   ├── db.py                  # PostgreSQL insertion
│   └── run_pipeline.py        # Scheduler — runs every 5 minutes
├── data_modeling/
│   ├── flights.sql            # enriched_flights table schema
│   └── create_schema.py       # Creates the table
├── dashboard/
│   └── app.py                 # Dash dashboard (auto-refreshes every 60s)
├── docker-compose.yml         # PostgreSQL + pgAdmin
├── requirements.txt
├── .env.example
└── README.md
```

---

## Dashboard

| Chart | Description |
|---|---|
| 🗺️ Live Flight Map | All active flights plotted on a North America map, coloured by altitude |
| 🏢 Top Airlines | Bar chart of most active airlines currently in the air |
| ⛅ Weather Conditions | Pie chart of weather under active flights |
| 📏 Altitude Distribution | Histogram of flight altitudes |
| 🌍 Origin Countries | Breakdown of flights by country of origin |

KPI cards show total active flights, countries, airlines, avg altitude, avg speed, and avg temperature at flight positions.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Flight data | OpenSky Network (free, no key needed) |
| Weather data | OpenWeatherMap (free tier) |
| Airline data | Lufthansa Open API (free tier) |
| Scheduling | APScheduler |
| Database | PostgreSQL 15 |
| Dashboard | Dash 2 + Plotly 5 |
| Containerization | Docker + docker-compose |

---

## Getting Started

### Prerequisites
- Python 3.10+
- Docker & Docker Compose
- Free API keys for OpenWeatherMap and Lufthansa (see below)

### 1. Clone the repository
```bash
git clone https://github.com/<your-username>/flight-enrichment.git
cd flight-enrichment
```

### 2. Set up environment variables
```bash
cp .env.example .env
```

Fill in your API keys:
- **OWM_API_KEY** — free at [openweathermap.org/api](https://openweathermap.org/api)
- **CLIENT_ID / CLIENT_SECRET** — free at [developer.lufthansa.com](https://developer.lufthansa.com/)
- OpenSky requires no key

### 3. Create virtual environment and install dependencies
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 4. Start the database
```bash
docker-compose up -d
```

---

## Running the Pipeline

**Step 1 — Create the table:**
```bash
python data_modeling/create_schema.py
```

**Step 2 — Start the pipeline (runs every 5 minutes automatically):**
```bash
python pipeline/run_pipeline.py
```

**Step 3 — Launch the dashboard (in a second Terminal tab):**
```bash
source venv/bin/activate
python dashboard/app.py
```

Open → [http://localhost:8050](http://localhost:8050)

---

## How the Enrichment Works

### Hop 1 — OpenSky Network
Queries the OpenSky `/states/all` endpoint with a North America bounding box. Returns raw transponder data: position, speed, altitude, heading.

### Hop 2 — OpenWeatherMap
For each flight's coordinates, fetches current weather conditions. Uses a 1-degree grid cache so nearby flights share the same API call — keeps usage well within the free tier's 60 calls/minute limit.

### Hop 3 — Lufthansa API
Derives the IATA airline code from the flight callsign prefix (e.g. `AAL` → American Airlines), then looks up the full airline name. Results are cached in-memory per run.

---

## Docker Services

| Service | URL | Credentials |
|---|---|---|
| PostgreSQL | `localhost:5432` | see `.env` |
| pgAdmin | http://localhost:5050 | admin@example.com / admin |
