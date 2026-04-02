#!/usr/bin/env python
# coding: utf-8

# # Default notebook
# 
# This default notebook is executed using a Lakeflow job as defined in resources/sample_job.job.yml.




import sys
print(sys.executable)



import requests
import sqlite3
import json
import os
import glob
import sys
from datetime import datetime, timezone

API_KEY = "b5cdd75cdaf74593ce17b7cf062cb3fe"
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
CITIES = ["Mumbai", "Delhi", "Bangalore", "Chennai", "Hyderabad"]
RAW = "/home/kaustubh/airflow/raw_json"
DB_FILE = "/home/kaustubh/airflow/weather.db"

def create_tables():

    CREATE_RAW = """
        CREATE TABLE IF NOT EXISTS raw_weather (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            city       TEXT    NOT NULL,
            fetched_at TEXT    DEFAULT (datetime('now')),
            raw_json   TEXT    NOT NULL
        );
        """
    
    CREATE_PARSED = """
        CREATE TABLE IF NOT EXISTS weather_observations (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            city            TEXT,
            country         TEXT,
            temperature_c   REAL,
            feels_like_c    REAL,
            temp_min_c      REAL,
            temp_max_c      REAL,
            humidity_pct    INTEGER,
            pressure_hpa    INTEGER,
            weather_main    TEXT,
            weather_desc    TEXT,
            wind_speed_ms   REAL,
            wind_direction  INTEGER,
            visibility_m    INTEGER,
            cloudiness_pct  INTEGER,
            observed_at     TEXT,
            fetched_at      TEXT    DEFAULT (datetime('now'))
        );
        """
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    cur.execute(CREATE_RAW)
    cur.execute(CREATE_PARSED)
    conn.commit()
    conn.close()


def fetch_weather(city,api_key=None):
    params = {
        "q": city,
        "appid": api_key or API_KEY,
        "units": "metric"
    }
    response = requests.get(BASE_URL, params=params, timeout=10)
    response.raise_for_status()
    return response.json()



def save_raw_json(data, city):
    os.makedirs(RAW,exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{city.lower().replace(' ','_')}_{timestamp}.json"
    filepath = os.path.join(RAW, filename)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    return filepath



def fetch_all_cities(api_key=None):
    results = []

    for city in CITIES:
        try:
            data = fetch_weather(city, api_key=api_key)
            filepath = save_raw_json(data,city)
            results.append((city,data))
        except requests.exceptions.HTTPError as e:
            print(f"{city} HTTP error occurred: {e}")
        except requests.exceptions.ConnectionError:
            print(f"{city} connection error occurred.")
        except Exception as e:
            print(f"{city} - {e}")
    return results



def load_raw_to_db(results):
    INSERT_RAW = """
        INSERT INTO raw_weather (city, raw_json)
        VALUES (?, ?)
        """
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    count = 0

    for city,data in results:
        try:
            raw_json_str = json.dumps(data)
            cur.execute(INSERT_RAW,(city,raw_json_str))
            count +=  1
        except Exception as e:
            print(f"Failed to insert {city}: {e}")

    conn.commit()
    conn.close()


def parse_weather_data(data):
    main = data.get("main", {})
    wind = data.get("wind",{})
    clouds = data.get("clouds", {})
    weather = data.get("weather", [{}])[0]
    sys = data.get("sys", {})
    return {
        "city": data.get("name"),
        "country": sys.get("country"),
        "temperature_c": main.get("temp"),
        "feels_like_c": main.get("feels_like"),
        "temp_min_c": main.get("temp_min"),
        "temp_max_c": main.get("temp_max"),
        "humidity_pct": main.get("humidity"),
        "pressure_hpa": main.get("pressure"),
        "weather_main": weather.get("main"),
        "weather_desc": weather.get("description"),
        "wind_speed_ms": wind.get("speed"),
        "wind_direction": wind.get("deg"),
        "visibility_m": data.get("visibility"),
        "cloudiness_pct": clouds.get("all"),
        "observed_at": datetime.fromtimestamp(data.get("dt",0), tz=timezone.utc)
    .strftime("%Y-%m-%d %H:%M:%S")
    }


def load_parsed_to_db(results):
    INSERT_PARSED = """
        INSERT INTO weather_observations
        (city, country, temperature_c, feels_like_c, temp_min_c, temp_max_c,
         humidity_pct, pressure_hpa, weather_main, weather_desc, wind_speed_ms,
         wind_direction, visibility_m, cloudiness_pct, observed_at)
        VALUES (
        :city, :country,
        :temperature_c, :feels_like_c, :temp_min_c, :temp_max_c,
        :humidity_pct, :pressure_hpa,
        :weather_main, :weather_desc,
        :wind_speed_ms, :wind_direction,
        :visibility_m, :cloudiness_pct,
        :observed_at
    );
        """
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    count = 0

    for city, data in results:
        try:
            record = parse_weather_data(data)
            cur.execute(INSERT_PARSED, record)
            print(f"Parsed & inserted  id={cur.lastrowid}  {city:<12} "
                  f"{record['temperature_c']}°C  {record['weather_desc']}")
            count += 1
        except Exception as e:
            print(f"Failed to insert {city}: {e}")

    conn.commit()
    conn.close()
    print(f"\n    {count} records inserted into weather_observations")


def run_query(query, description):
    """Run a SQL query and print results as a formatted table."""
    print(f"\n--- {description} ---")
 
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row   # allows column access by name
    cur  = conn.cursor()
    cur.execute(query)
    rows    = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    conn.close()
 
    if not rows:
        print("  (no data found)")
        return
 
    # calculate column widths for neat printing
    col_widths = [
        max(len(str(col)), max(len(str(row[col])) for row in rows))
        for col in columns
    ]
 
    # print header
    header = "  ".join(str(col).ljust(col_widths[i]) for i, col in enumerate(columns))
    print(header)
    print("-" * len(header))
 
    # print rows
    for row in rows:
        print("  ".join(str(row[col]).ljust(col_widths[i]) for i, col in enumerate(columns)))
 
    print(f"({len(rows)} rows)")



def query_results():
    """Run sample queries to verify data loaded correctly."""
    print("\n[5] Querying results...")
 
    # All observations sorted by temperature
    run_query("""
        SELECT city, country, temperature_c, feels_like_c,
               humidity_pct, weather_desc, wind_speed_ms
        FROM weather_observations
        ORDER BY temperature_c DESC;
    """, "All observations (hottest first)")
 
    # Summary statistics
    run_query("""
        SELECT
            COUNT(*)                        AS total_records,
            ROUND(AVG(temperature_c), 2)    AS avg_temp_c,
            MAX(temperature_c)              AS max_temp_c,
            MIN(temperature_c)              AS min_temp_c,
            ROUND(AVG(humidity_pct), 1)     AS avg_humidity_pct,
            ROUND(AVG(wind_speed_ms), 2)    AS avg_wind_ms
        FROM weather_observations;
    """, "Summary statistics")
 
    # Hottest and coldest city
    run_query("""
        SELECT city, temperature_c, weather_desc
        FROM weather_observations
        ORDER BY temperature_c DESC
        LIMIT 1;
    """, "Hottest city")
 
    run_query("""
        SELECT city, temperature_c, weather_desc
        FROM weather_observations
        ORDER BY temperature_c ASC
        LIMIT 1;
    """, "Coldest city")
 
    # Query raw JSON stored as text
    run_query("""
        SELECT city, fetched_at,
               LENGTH(raw_json) AS json_size_chars
        FROM raw_weather
        ORDER BY city;
    """, "Raw JSON records")


def verify_database():
    """Print a summary of what's in the database."""
 
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
 
    # list all tables
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cur.fetchall()]
    print(f"    Tables found: {tables}")
 
    # row counts
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        count = cur.fetchone()[0]
        print(f"    {table:<25} → {count} rows")
 
    conn.close()
    print(f"\n Database file: {DB_FILE} "
          f"({os.path.getsize(DB_FILE)} bytes)")



def main():
    print("   Weather API → Raw JSON → SQLite Pipeline")
    # Step 1: Create tables (SQLite creates the .db file automatically)
    create_tables()
    # Step 2: Fetch from API and save raw JSON files
    results = fetch_all_cities()

    if not results:
        print("\n No data fetched. Check your API key and internet connection.")
        return
    # Step 3: Load raw JSON into SQLite
    load_raw_to_db(results)

    # Step 4: Parse and load structured data
    load_parsed_to_db(results)

    # Step 5: Query and display results
    query_results()

    # Step 6: Verify database
    verify_database()

    print("Pipeline complete!")
    print(f"   Raw JSON files : ./{RAW}/")
    print(f"   SQLite DB file : ./{DB_FILE}")

if __name__ == "__main__":
    main()


