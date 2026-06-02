"""
UK Weather Pipeline — Ingestion Script
=======================================
Fetches daily weather data for 10 UK cities from the Open-Meteo API
and loads it into Snowflake RAW schema.

Usage:
    python ingest_weather.py                  # today only
    python ingest_weather.py --backfill 365   # backfill last N days

Requirements:
    pip install requests snowflake-connector-python python-dotenv
"""

import os
import sys
import logging
import argparse
import requests
from datetime import date, timedelta
from dotenv import load_dotenv
import snowflake.connector

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Config — 10 UK cities with coordinates
# ──────────────────────────────────────────────
UK_CITIES = [
    {"name": "London",      "lat": 51.5074,  "lon": -0.1278},
    {"name": "Manchester",  "lat": 53.4808,  "lon": -2.2426},
    {"name": "Birmingham",  "lat": 52.4862,  "lon": -1.8904},
    {"name": "Leeds",       "lat": 53.8008,  "lon": -1.5491},
    {"name": "Glasgow",     "lat": 55.8642,  "lon": -4.2518},
    {"name": "Edinburgh",   "lat": 55.9533,  "lon": -3.1883},
    {"name": "Cardiff",     "lat": 51.4816,  "lon": -3.1791},
    {"name": "Bristol",     "lat": 51.4545,  "lon": -2.5879},
    {"name": "Sheffield",   "lat": 53.3811,  "lon": -1.4701},
    {"name": "Liverpool",   "lat": 53.4084,  "lon": -2.9916},
]

# Open-Meteo daily variables to fetch
WEATHER_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "windspeed_10m_max",
    "windgusts_10m_max",
    "winddirection_10m_dominant",
    "weathercode",
    "sunrise",
    "sunset",
]

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# ──────────────────────────────────────────────
# Step 1 — Fetch weather from Open-Meteo
# ──────────────────────────────────────────────
def fetch_weather(city: dict, start_date: str, end_date: str) -> list[dict]:
    """
    Calls Open-Meteo API for one city over a date range.
    Returns a list of row dicts, one per day.
    """
    params = {
        "latitude":        city["lat"],
        "longitude":       city["lon"],
        "daily":           ",".join(WEATHER_VARIABLES),
        "timezone":        "Europe/London",
        "start_date":      start_date,
        "end_date":        end_date,
    }

    resp = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    daily = data.get("daily", {})
    dates = daily.get("time", [])
    n = len(dates)

    rows = []
    for i in range(n):
        row = {
            "city_name":                    city["name"],
            "latitude":                     city["lat"],
            "longitude":                    city["lon"],
            "weather_date":                 dates[i],
            "temperature_max_c":            daily.get("temperature_2m_max",              [None]*n)[i],
            "temperature_min_c":            daily.get("temperature_2m_min",              [None]*n)[i],
            "temperature_mean_c":           daily.get("temperature_2m_mean",             [None]*n)[i],
            "precipitation_mm":             daily.get("precipitation_sum",               [None]*n)[i],
            "rain_mm":                      daily.get("rain_sum",                        [None]*n)[i],
            "snowfall_cm":                  daily.get("snowfall_sum",                    [None]*n)[i],
            "windspeed_max_kmh":            daily.get("windspeed_10m_max",               [None]*n)[i],
            "windgusts_max_kmh":            daily.get("windgusts_10m_max",               [None]*n)[i],
            "wind_direction_dominant_deg":  daily.get("winddirection_10m_dominant",      [None]*n)[i],
            "weather_code":                 daily.get("weathercode",                     [None]*n)[i],
            "sunrise":                      daily.get("sunrise",                         [None]*n)[i],
            "sunset":                       daily.get("sunset",                          [None]*n)[i],
        }
        rows.append(row)

    log.info(f"  {city['name']}: fetched {n} day(s) from {start_date} to {end_date}")
    return rows


# ──────────────────────────────────────────────
# Step 2 — Connect to Snowflake
# ──────────────────────────────────────────────
def get_snowflake_conn():
    """
    Reads credentials from environment variables (set in .env or GitHub Secrets).
    Returns an open Snowflake connection.
    """
    load_dotenv()  # loads .env file if present locally

    conn = snowflake.connector.connect(
        account=   os.environ["SNOWFLAKE_ACCOUNT"],    # e.g. abc123.eu-west-1
        user=      os.environ["SNOWFLAKE_USER"],
        password=  os.environ["SNOWFLAKE_PASSWORD"],
        warehouse= os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=  os.environ.get("SNOWFLAKE_DATABASE",  "WEATHER_DB"),
        schema=    os.environ.get("SNOWFLAKE_SCHEMA",    "RAW"),
        role=      os.environ.get("SNOWFLAKE_ROLE",      "SYSADMIN"),
    )
    log.info("Snowflake connection established")
    return conn


# ──────────────────────────────────────────────
# Step 3 — Create RAW table if it doesn't exist
# ──────────────────────────────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS RAW.RAW_WEATHER (
    city_name                   VARCHAR(50),
    latitude                    FLOAT,
    longitude                   FLOAT,
    weather_date                DATE,
    temperature_max_c           FLOAT,
    temperature_min_c           FLOAT,
    temperature_mean_c          FLOAT,
    precipitation_mm            FLOAT,
    rain_mm                     FLOAT,
    snowfall_cm                 FLOAT,
    windspeed_max_kmh           FLOAT,
    windgusts_max_kmh           FLOAT,
    wind_direction_dominant_deg FLOAT,
    weather_code                INTEGER,
    sunrise                     VARCHAR(25),
    sunset                      VARCHAR(25),
    _loaded_at                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
"""

def ensure_table(cursor):
    cursor.execute(CREATE_TABLE_SQL)
    log.info("RAW.RAW_WEATHER table ready")


# ──────────────────────────────────────────────
# Step 4 — Upsert rows (merge on city + date)
# ──────────────────────────────────────────────
MERGE_SQL = """
MERGE INTO RAW.RAW_WEATHER AS target
USING (
    SELECT
        %(city_name)s                    AS city_name,
        %(latitude)s                     AS latitude,
        %(longitude)s                    AS longitude,
        %(weather_date)s::DATE           AS weather_date,
        %(temperature_max_c)s            AS temperature_max_c,
        %(temperature_min_c)s            AS temperature_min_c,
        %(temperature_mean_c)s           AS temperature_mean_c,
        %(precipitation_mm)s             AS precipitation_mm,
        %(rain_mm)s                      AS rain_mm,
        %(snowfall_cm)s                  AS snowfall_cm,
        %(windspeed_max_kmh)s            AS windspeed_max_kmh,
        %(windgusts_max_kmh)s            AS windgusts_max_kmh,
        %(wind_direction_dominant_deg)s  AS wind_direction_dominant_deg,
        %(weather_code)s                 AS weather_code,
        %(sunrise)s                      AS sunrise,
        %(sunset)s                       AS sunset
) AS source
ON  target.city_name    = source.city_name
AND target.weather_date = source.weather_date
WHEN MATCHED THEN UPDATE SET
    temperature_max_c           = source.temperature_max_c,
    temperature_min_c           = source.temperature_min_c,
    temperature_mean_c          = source.temperature_mean_c,
    precipitation_mm            = source.precipitation_mm,
    rain_mm                     = source.rain_mm,
    snowfall_cm                 = source.snowfall_cm,
    windspeed_max_kmh           = source.windspeed_max_kmh,
    windgusts_max_kmh           = source.windgusts_max_kmh,
    wind_direction_dominant_deg = source.wind_direction_dominant_deg,
    weather_code                = source.weather_code,
    sunrise                     = source.sunrise,
    sunset                      = source.sunset,
    _loaded_at                  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    city_name, latitude, longitude, weather_date,
    temperature_max_c, temperature_min_c, temperature_mean_c,
    precipitation_mm, rain_mm, snowfall_cm,
    windspeed_max_kmh, windgusts_max_kmh, wind_direction_dominant_deg,
    weather_code, sunrise, sunset
) VALUES (
    source.city_name, source.latitude, source.longitude, source.weather_date,
    source.temperature_max_c, source.temperature_min_c, source.temperature_mean_c,
    source.precipitation_mm, source.rain_mm, source.snowfall_cm,
    source.windspeed_max_kmh, source.windgusts_max_kmh, source.wind_direction_dominant_deg,
    source.weather_code, source.sunrise, source.sunset
);
"""

def load_rows(cursor, rows: list[dict]) -> int:
    """Upsert a list of row dicts. Returns number of rows processed."""
    for row in rows:
        cursor.execute(MERGE_SQL, row)
    return len(rows)


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
def run(backfill_days: int = 0):
    today = date.today()

    if backfill_days > 0:
        start = today - timedelta(days=backfill_days)
    else:
        start = today

    start_str = start.isoformat()
    end_str   = today.isoformat()

    log.info(f"Pipeline starting — date range: {start_str} → {end_str}")
    log.info(f"Cities: {len(UK_CITIES)}  |  Variables: {len(WEATHER_VARIABLES)}")

    # --- Fetch all cities ---
    all_rows = []
    for city in UK_CITIES:
        try:
            rows = fetch_weather(city, start_str, end_str)
            all_rows.extend(rows)
        except Exception as e:
            log.error(f"  FAILED fetching {city['name']}: {e}")

    log.info(f"Total rows fetched: {len(all_rows)}")

    if not all_rows:
        log.warning("No rows to load — exiting")
        sys.exit(1)

    # --- Load to Snowflake ---
    conn = get_snowflake_conn()
    try:
        cur = conn.cursor()
        ensure_table(cur)
        loaded = load_rows(cur, all_rows)
        conn.commit()
        log.info(f"Successfully loaded {loaded} rows into RAW.RAW_WEATHER")
    except Exception as e:
        conn.rollback()
        log.error(f"Snowflake load failed: {e}")
        raise
    finally:
        conn.close()
        log.info("Snowflake connection closed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UK Weather Pipeline Ingestion")
    parser.add_argument(
        "--backfill",
        type=int,
        default=0,
        metavar="DAYS",
        help="Number of historical days to backfill (default: 0 = today only)",
    )
    args = parser.parse_args()
    run(backfill_days=args.backfill)
