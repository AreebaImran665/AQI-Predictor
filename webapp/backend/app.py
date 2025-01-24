import os
import requests
from datetime import datetime
import pandas as pd
import hopsworks
import joblib
from fastapi import FastAPI
from pydantic import BaseModel
import numpy as np

app = FastAPI()

# Pydantic model for the response data
class AQIPredictionResponse(BaseModel):
    day: int
    predicted_aqi: float

# URLs for data
URL_POLLUTION_FORECAST = "http://api.openweathermap.org/data/2.5/air_pollution/forecast"
URL_WEATHER_FORECAST = "https://api.open-meteo.com/v1/forecast"

# Fetch API keys from environment variables
API_KEY = os.getenv("OPEN_WEATHER_API")
hopsworks_api_key = os.getenv("HOPSWORKS_API_KEY")

# Location
LATITUDE = 24.8607
LONGITUDE = 67.0011

# def fetch_and_predict_aqi_data(day_count, model_name):
def fetch_and_predict_aqi_data(day_count, model_name, latitude, longitude):

    try:
        # Initialize Hopsworks Feature Store and Model Registry
        project = hopsworks.login(api_key_value="hopsworks_api_key")
        mr = project.get_model_registry()

        # Fetch all models with the specified name and get the latest version
        models = mr.get_models(name=model_name)
        latest_model = max(models, key=lambda m: m.version)  # Get the latest version
        model_version = latest_model.version
        print(f"Using model version {model_version}")

        # Load the trained model from Model Registry
        model_dir = mr.get_model(model_name, version=model_version).download()
        model = joblib.load(f"{model_dir}/rf_model.pkl")

        # Forecast URLs and Parameters
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
        response_pollution = requests.get(URL_POLLUTION_FORECAST, params=params_pollution)
        response_pollution.raise_for_status()
        air_data = response_pollution.json()

        response_weather = requests.get(URL_WEATHER_FORECAST, params=params_weather)
        response_weather.raise_for_status()
        weather_data = response_weather.json()

        # Process forecast data
        data_rows = []
        previous_aqi = None

        for i in range(1, day_count + 1):
            if "list" not in air_data or not air_data["list"]:
                print(f"No air pollution forecast data for day {i}")
                continue
            
            aqi = air_data["list"][0]["main"]["aqi"]
            components = air_data["list"][0]["components"]
            timestamp = air_data["list"][0]["dt"]
            readable_time = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')

            # Compute AQI Change Rate
            aqi_change_rate = 0 if previous_aqi is None else aqi - previous_aqi

            # Extract Weather Data
            max_temp = weather_data["daily"]["temperature_2m_max"][i - 1]
            min_temp = weather_data["daily"]["temperature_2m_min"][i - 1]
            precipitation = weather_data["daily"]["precipitation_sum"][i - 1]
            wind_speed = weather_data["daily"]["windspeed_10m_max"][i - 1]

            # Combine into a single row
            row = {
                "readable_time": readable_time,
                "day_offset": i,
                "hour": datetime.utcfromtimestamp(timestamp).hour,
                "day": datetime.utcfromtimestamp(timestamp).day,
                "month": datetime.utcfromtimestamp(timestamp).month,
                "latitude": LATITUDE,
                "longitude": LONGITUDE,
                "aqi": aqi,  # This is the target column, not a feature
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

            data_rows.append(row)
            previous_aqi = aqi

        # Convert to DataFrame
        forecast_df = pd.DataFrame(data_rows)

        # Drop the 'aqi' column, as it is the target (not a feature)
        features_df = forecast_df.drop(['readable_time', 'day_offset', 'latitude', 'longitude', 'aqi'], axis=1)

        # Predict AQI using the trained model
        if not features_df.empty:
            predictions = model.predict(features_df)
            forecast_df["predicted_aqi"] = predictions

            # Save the forecast to a CSV
            forecast_df.to_csv("forecast_data.csv", index=False)
            print("AQI forecast saved successfully!")
            return forecast_df[['day_offset', 'predicted_aqi']]

        else:
            print("No forecast data to process.")
            return pd.DataFrame()

    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

@app.get("/predict")
def predict_aqi_api():
    # forecast_df = fetch_and_predict_aqi_data(day_count=3, model_name="random_forest", latitude=LATITUDE, longitude=LONGITUDE)
    
    forecast_df = fetch_and_predict_aqi_data(
        day_count=3,
        model_name="random_forest",
        latitude=LATITUDE,
        longitude=LONGITUDE,
        # api_key=API_KEY
    )

    # Check if the forecast data is available and return the result
    if not forecast_df.empty:
        predictions = forecast_df[['day_offset', 'predicted_aqi']].to_dict(orient="records")
        return {"predictions": predictions}
    else:
        return {"error": "No forecast data available for prediction."}
