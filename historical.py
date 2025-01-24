# -----------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------
# ----------------  Backfilling Weather and Pollutant Historical Data for 400 days --------------------
# -----------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------

# Backfilling Weather and Pollutant Historical Data for 400 days


import requests
from datetime import datetime, timedelta, timezone
import pandas as pd
import hopsworks

# API Keys and URLs
API_KEY = "d0daaf650425edd685b9e1831cf94b32"
URL_POLLUTION = "http://api.openweathermap.org/data/2.5/air_pollution/history"
URL_WEATHER = "https://archive-api.open-meteo.com/v1/archive"

# Location
LATITUDE = 24.8607
LONGITUDE = 67.0011

# Initialize Hopsworks Feature Store
project = hopsworks.login(api_key_value="n7jRofG3Y9HUQ8Zi.hUbA78pZislL2kOnmPCnOPYberwqZf798dkc1ebR1czoVZ3LwMYsPvKonujAjQkY")
# project = hopsworks.login(api_key_value="Xzjv1TKrbxrZxlPu.nu12KCtSTikBGJVCU0BGmWXTe5ppztGqhtlZp4JKhlduh8GwjQm2YyuSPvBzLJyA")

fs = project.get_feature_store()


# Insert into Feature Store
feature_group = fs.get_feature_group(name="weather_and_pollutant_data", version=1)


# Function to fetch data
def fetch_data(url_pollution, params_pollution, url_weather, params_weather):
    try:
        response_pollution = requests.get(url_pollution, params=params_pollution)
        response_pollution.raise_for_status()
        air_data = response_pollution.json()
        print(f"======>pollution{air_data}")

        response_weather = requests.get(url_weather, params=params_weather)
        response_weather.raise_for_status()
        weather_data = response_weather.json()
        print(f"======>weather{weather_data}")

        return air_data, weather_data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None, None

# Function to process and save data into a DataFrame
def process_data(air_data, weather_data, day_offset, previous_aqi):
    # Check if air_data['list'] is empty
    if not air_data["list"]:
        print(f"No pollution data for day {day_offset}")
        return None, previous_aqi  # Skip this iteration if no pollution data

    # Extract Air Pollution Data
    coordinates = air_data["coord"]
    aqi = air_data["list"][0]["main"]["aqi"]
    components = air_data["list"][0]["components"]
    timestamp = air_data["list"][0]["dt"]
    readable_time = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')  # Only date

    # Compute AQI Change Rate
    aqi_change_rate = 0 if previous_aqi is None else aqi - previous_aqi

    # Extract Weather Data
    max_temp = weather_data["daily"]["temperature_2m_max"][0]
    min_temp = weather_data["daily"]["temperature_2m_min"][0]
    precipitation = weather_data["daily"].get("precipitation_sum", [0])[0]
    wind_speed = weather_data["daily"]["windspeed_10m_max"][0]
    if precipitation is None:
        precipitation = 0.0
    # Compute additional features
    hour = datetime.utcfromtimestamp(timestamp).hour
    day = datetime.utcfromtimestamp(timestamp).day
    month = datetime.utcfromtimestamp(timestamp).month

    # Combine into a single row
    row = {
        "readable_time": readable_time,
        "day_offset": int(day_offset),
        "hour": int(hour),
        "day": int(day),
        "month": int(month),
        "latitude": coordinates["lat"],
        "longitude": coordinates["lon"],
        "aqi": int(aqi),
        "aqi_change_rate": int(aqi_change_rate),
        "co": float(components["co"]),
        "no": int(components["no"]),
        "no2": float(components["no2"]),
        "o3": float(components["o3"]),
        "so2": float(components["so2"]),
        "pm2_5": float(components["pm2_5"]),
        "pm10": float(components["pm10"]),
        "nh3": float(components["nh3"]),
        "max_temp": float(max_temp),
        "min_temp": float(min_temp),
        "precipitation": float(precipitation),
        "max_wind_speed": float(wind_speed)
    }

    return row, aqi

# Function to fetch historical data and prepare DataFrame
def fetch_historical_data(day_offset):
    data_rows = []
    previous_aqi = None  # Initialize to None before processing

    for i in range(1, day_offset + 1):
        print('----------------------------------------------------------------------')
        print(i)
        print('----------------------------------------------------------------------')

        # Generate timestamps for the day
        target_day = datetime.now(timezone.utc) - timedelta(days=i)
        start_time = int(target_day.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        end_time = start_time + 86400  # 86400 seconds in a day

        # API Parameters
        params_pollution = {
            "lat": LATITUDE,
            "lon": LONGITUDE,
            "start": start_time,
            "end": end_time,
            "appid": API_KEY
        }
        params_weather = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "start_date": target_day.strftime("%Y-%m-%d"),
            "end_date": target_day.strftime("%Y-%m-%d"),
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max",
            "timezone": "Asia/Karachi"
        }

        # Fetch data
        air_data, weather_data = fetch_data(URL_POLLUTION, params_pollution, URL_WEATHER, params_weather)

        # Check data and process
        if air_data and weather_data and "daily" in weather_data and len(weather_data["daily"]["time"]) > 0:
            row, previous_aqi = process_data(air_data, weather_data, i, previous_aqi)
            if row:  
                data_rows.append(row)
        else:
            print(f"Data missing for day {i}.")

    # Return DataFrame only if data_rows is not empty
    if data_rows:
        return pd.DataFrame(data_rows)
    else:
        print("No valid data found to create a DataFrame.")
        return pd.DataFrame()  # Return an empty DataFrame if no data


# Main Execution
data_df = fetch_historical_data(400)  # Pass the required day offset


# Ensure that data_df is not None before saving
if not data_df.empty:
    data_df.to_csv("aqi_data.csv", index=False)
    # feature_group = fs.get_feature_group(name="weather_and_pollutant_data", version=1)
    feature_group.insert(data_df)
    print("Historical data successfully inserted into the feature store!")
else:
    print("No data to save.")
