import pandas as pd
import requests
import zipfile
import io
import os
import time
import shutil
import redis
from datetime import datetime, timedelta
import pytz

def process_and_store_schedules():
    """
    Downloads GTFS data, processes it for today's and tomorrow's bus schedules
    in Box Hill, and stores the result in Redis.
    """
    # --- REVISED: Use the writable /tmp directory for file operations ---
    main_extract_path = "/tmp/gtfs_processing"
    
    # --- Setup ---
    url = "https://opendata.transport.vic.gov.au/dataset/3f4e292e-7f8a-4ffe-831f-1953be0fe448/resource/e4966d78-dc64-4a1d-a751-2470c9eaf034/download/gtfs.zip"
    filter_keyword = "Box Hill"
    REDIS_URL = os.getenv("REDIS_URL")
    REDIS_KEY_NAME = "schedules:box_hill:today_and_tomorrow"

    # Clean up previous run's temp files if they exist
    if os.path.exists(main_extract_path):
        shutil.rmtree(main_extract_path)
    os.makedirs(main_extract_path)

    print("Righto, let's get today's and tomorrow's schedules...")
    start_time = time.perf_counter()

    # --- The rest of your script remains the same ---
    print("Downloading and extracting GTFS data...")
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        z.extract(os.path.join("4", "google_transit.zip"), path=main_extract_path)
    bus_txt_files_path = os.path.join(main_extract_path, "bus_data")
    with zipfile.ZipFile(os.path.join(main_extract_path, "4", "google_transit.zip"), 'r') as nested_z:
        nested_z.extractall(bus_txt_files_path)
    
    stops_df = pd.read_csv(os.path.join(bus_txt_files_path, 'stops.txt'))
    stop_times_df = pd.read_csv(os.path.join(bus_txt_files_path, 'stop_times.txt'))
    trips_df = pd.read_csv(os.path.join(bus_txt_files_path, 'trips.txt'))
    routes_df = pd.read_csv(os.path.join(bus_txt_files_path, 'routes.txt'))
    calendar_dates_df = pd.read_csv(os.path.join(bus_txt_files_path, 'calendar_dates.txt'))

    melbourne_tz = pytz.timezone('Australia/Melbourne')
    now_melbourne = datetime.now(melbourne_tz)
    
    today_str = now_melbourne.strftime('%Y%m%d')
    tomorrow_str = (now_melbourne + timedelta(days=1)).strftime('%Y%m%d')
    dates_to_check = [int(today_str), int(tomorrow_str)]
    
    print(f"Filtering for dates: {today_str} and {tomorrow_str}")

    active_services_df = calendar_dates_df[
        (calendar_dates_df['date'].isin(dates_to_check)) &
        (calendar_dates_df['exception_type'] == 1)
    ]
    
    if active_services_df.empty:
        print("Bummer! No services found running for today or tomorrow.")
        shutil.rmtree(main_extract_path)
        return

    print("Joining and filtering schedules...")
    stops_filtered = stops_df[stops_df['stop_name'].str.contains(filter_keyword, case=False, na=False)]
    
    all_schedules = pd.merge(stops_filtered, stop_times_df, on='stop_id') \
                      .merge(trips_df, on='trip_id') \
                      .merge(routes_df, on='route_id')

    active_box_hill_schedules = pd.merge(all_schedules, active_services_df[['service_id']], on='service_id')
    final_columns = [
        'trip_id', 'stop_sequence', 'route_id', 'route_short_name', 'route_long_name',
        'direction_id', 'service_id', 'trip_headsign', 'stop_name', 'stop_id',
        'stop_lat', 'stop_lon', 'departure_time'
    ]
    final_df = active_box_hill_schedules[final_columns].sort_values(by=['route_short_name', 'departure_time'])
    print(f"Found {len(final_df)} bus services for 'Box Hill' running today and tomorrow.")

    if REDIS_URL is None:
        print("❌ ERROR: REDIS_URL environment variable not set.")
        return

    print(f"Storing result in Redis under key: '{REDIS_KEY_NAME}'")
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        schedules_json = final_df.to_json(orient='records')
        r.set(REDIS_KEY_NAME, schedules_json, ex=90000)
        print("✅ Successfully stored data in Redis.")

    except Exception as e:
        print(f"❌ An error occurred during Redis operation: {e}")

    end_time = time.perf_counter()
    shutil.rmtree(main_extract_path)
    print(f"\nTotal time taken: {end_time - start_time:.2f} seconds")

# This allows the script to be imported without running the main function
if __name__ == '__main__':
    process_and_store_schedules()
