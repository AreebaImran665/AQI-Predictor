# Loads the model and features from the Feature Store

import requests
from datetime import datetime, timedelta, timezone
import pandas as pd
import hopsworks
import joblib

API_KEY = "d0daaf650425edd685b9e1831cf94b32"
# Forecast URLs
URL_POLLUTION_FORECAST = "http://api.openweathermap.org/data/2.5/air_pollution/forecast"
URL_WEATHER_FORECAST = "https://api.open-meteo.com/v1/forecast"

# Location
LATITUDE = 24.8607
LONGITUDE = 67.0011

# Initialize Hopsworks Feature Store and Model Registry
project = hopsworks.login(api_key_value="n7jRofG3Y9HUQ8Zi.hUbA78pZislL2kOnmPCnOPYberwqZf798dkc1ebR1czoVZ3LwMYsPvKonujAjQkY")
fs = project.get_feature_store()
mr = project.get_model_registry()

# Load the trained model from Model Registry
model_name = "random_forest"
model_version = 1
model_dir = mr.get_model(model_name, version=model_version).download()

model = joblib.load(f"{model_dir}/rf_model.pkl")

def fetch_forecast_data(url_pollution, params_pollution, url_weather, params_weather):
    try:
        response_pollution = requests.get(url_pollution, params=params_pollution)
        response_pollution.raise_for_status()
        air_data = response_pollution.json()
        print(f"======>pollution forecast: {air_data}")

        response_weather = requests.get(url_weather, params=params_weather)
        response_weather.raise_for_status()
        weather_data = response_weather.json()
        print(f"======>weather forecast: {weather_data}")

        return air_data, weather_data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching forecast data: {e}")
        return None, None

# Function to process forecast data
def process_forecast_data(air_data, weather_data, day_offset, previous_aqi):
    if "list" not in air_data or not air_data["list"]:
        print(f"No air pollution forecast data for day {day_offset}")
        return None, previous_aqi

    # Extract Air Pollution Data
    aqi = air_data["list"][0]["main"]["aqi"]
    components = air_data["list"][0]["components"]
    timestamp = air_data["list"][0]["dt"]
    readable_time = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')

    # Compute AQI Change Rate
    aqi_change_rate = 0 if previous_aqi is None else aqi - previous_aqi

    # Compute additional features
    hour = datetime.utcfromtimestamp(timestamp).hour
    day = datetime.utcfromtimestamp(timestamp).day
    month = datetime.utcfromtimestamp(timestamp).month

    # Extract Weather Data
    max_temp = weather_data["daily"]["temperature_2m_max"][day_offset - 1]
    min_temp = weather_data["daily"]["temperature_2m_min"][day_offset - 1]
    precipitation = weather_data["daily"]["precipitation_sum"][day_offset - 1]
    wind_speed = weather_data["daily"]["windspeed_10m_max"][day_offset - 1]

    # Compute additional features
    hour = datetime.utcfromtimestamp(timestamp).hour
    day = datetime.utcfromtimestamp(timestamp).day
    month = datetime.utcfromtimestamp(timestamp).month

    # Combine into a single row
    row = {
        "readable_time": readable_time,
        "day_offset": day_offset,
        "hour": hour,
        "day": day,
        "month": month,
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "aqi": aqi,
        "aqi_change_rate": aqi_change_rate,
        "co": components["co"],
        "no": components["no"],
        "no2": components["no2"],
        "o3": components["o3"],
        "so2": components["so2"],
        "pm2_5": components["pm2_5"],
        "pm10": components["pm10"],
        "nh3": components["nh3"],
        "max_temp": max_temp,
        "min_temp": min_temp,
        "precipitation": precipitation,
        "max_wind_speed": wind_speed
    }

    return row, aqi

# Function to fetch and prepare forecast data
def fetch_forecast_aqi_data(day_count):
    data_rows = []
    previous_aqi = None

    # API Parameters
    params_pollution = {
        "lat": LATITUDE,
        "lon": LONGITUDE,
        "appid": API_KEY
    }
    params_weather = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max",
        "timezone": "Asia/Karachi"
    }

    # Fetch forecast data
    air_data, weather_data = fetch_forecast_data(URL_POLLUTION_FORECAST, params_pollution, URL_WEATHER_FORECAST, params_weather)

    if air_data and weather_data and "daily" in weather_data and len(weather_data["daily"]["time"]) >= day_count:
        for i in range(1, day_count + 1):
            row, previous_aqi = process_forecast_data(air_data, weather_data, i, previous_aqi)
            if row:
                data_rows.append(row)
    else:
        print("Insufficient forecast data.")

    # Return DataFrame
    if data_rows:
        return pd.DataFrame(data_rows)
    else:
        print("No valid forecast data found.")
        return pd.DataFrame()

# Predict AQI using Hopsworks Model Registry
def predict_aqi(data):
    try:
        data = data.drop(['readable_time', 'day_offset', 'LATITUDE', 'longitude'], axis=1)
        predictions = model.predict(data)
        data["aqi"] = predictions
        return data

    except Exception as e:
        print(f"Error during AQI prediction: {e}")
        return data

# Main Execution
forecast_df = fetch_forecast_aqi_data(3)  # Fetch 3-day forecast data

if not forecast_df.empty:
    forecast_df = predict_aqi(forecast_df)
    forecast_df.to_csv("forecast_aqi_data.csv", index=False)
    print("3-Days AQI forecast saved successfully!")
    print(forecast_df)
else:
    print("No forecast data to process.")