# -----------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------
# ------------------------------------  Feature Script 1   --------------------------------------------
# -------------------------Feature Pipeline Current Data into Hopswork --------------------------------
# -----------------------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------------------

import os
import requests
from datetime import datetime
import hopsworks

# API URLs
open_weather_url = "http://api.openweathermap.org/data/2.5/air_pollution"
open_meteo_url = "https://api.open-meteo.com/v1/forecast"

# Fetch API keys from environment variables
open_weather_api_key = os.getenv("OPEN_WEATHER_API")
hopsworks_api_key = os.getenv("HOPSWORKS_API_KEY")

# Location details
latitude = 24.8607
longitude = 67.0011

# Default values
aqi_change_rate = 0
day_offset = 0

# Initialize Hopsworks connection
project = hopsworks.login(api_key_value=hopsworks_api_key)
fs = project.get_feature_store()

# Fetch Air Pollution Data
try:
    air_pollution_response = requests.get(open_weather_url, params={
        "lat": latitude,
        "lon": longitude,
        "appid": open_weather_api_key
    })
    air_pollution_response.raise_for_status()
    air_pollution_data = air_pollution_response.json()
except requests.exceptions.RequestException as e:
    print(f"Error fetching Air Pollution Data: {e}")

# Fetch Weather Data
try:
    weather_response = requests.get(open_meteo_url, params={
        "latitude": latitude,
        "longitude": longitude,
        "current_weather": True,
        "timezone": "Asia/Karachi",
        "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "windspeed_10m_max"]
    })
    weather_response.raise_for_status()
    weather_data = weather_response.json()
except requests.exceptions.RequestException as e:
    print(f"Error fetching Weather Data: {e}")

# Extract Air Pollution Data
timestamp = air_pollution_data["list"][0]["dt"]
readable_time = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
hour = datetime.utcfromtimestamp(timestamp).hour
day = datetime.utcfromtimestamp(timestamp).day
month = datetime.utcfromtimestamp(timestamp).month

aqi = air_pollution_data["list"][0]["main"]["aqi"]
components = air_pollution_data["list"][0]["components"]
co = components.get("co", 0)
no = components.get("no", 0)
no2 = components.get("no2", 0)
o3 = components.get("o3", 0)
so2 = components.get("so2", 0)
pm2_5 = components.get("pm2_5", 0)
pm10 = components.get("pm10", 0)
nh3 = components.get("nh3", 0)

# Extract Weather Data
daily_data = weather_data.get("daily", {})
max_temp = daily_data.get("temperature_2m_max", [0])[0]
min_temp = daily_data.get("temperature_2m_min", [0])[0]
precipitation = daily_data.get("precipitation_sum", [0])[0]
max_wind_speed = daily_data.get("windspeed_10m_max", [0])[0]
if precipitation is None:
    precipitation = 0.0
# Combine data into a single dictionary
data = {
    "readable_time": readable_time,
    "day_offset": int(day_offset),
    "hour": int(hour),
    "day": int(day),
    "month": int(month),
    "latitude": latitude,
    "longitude": longitude,
    "aqi": int(aqi),
    "aqi_change_rate": int(aqi_change_rate),
    "co": float(co),
    "no": int(no),
    "no2": float(no2),
    "o3": float(o3),
    "so2": float(so2),
    "pm2_5": float(pm2_5),
    "pm10": float(pm10),
    "nh3": float(nh3),
    "max_temp": float(max_temp),
    "min_temp": float(min_temp),
    "precipitation": float(precipitation),
    "max_wind_speed": float(max_wind_speed)
}

# Convert to Pandas DataFrame
import pandas as pd

data_df = pd.DataFrame([data])

# Create or Get Feature Group
feature_group = fs.get_or_create_feature_group(
    name="weather_and_pollutant_data",
    version=1,
    primary_key=["readable_time"],
    description="weather and air pollution data of 400 days",
    online_enabled=True
)

# Insert data into the feature store
feature_group.insert(data_df)

print("Data successfully inserted into the Hopsworks Feature Store!")