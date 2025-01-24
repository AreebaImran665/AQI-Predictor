import streamlit as st
import requests
import pandas as pd

# URL for FastAPI backend
API_URL = "http://127.0.0.1:8000/predict"  # Replace with your FastAPI server URL

# Streamlit app
st.title("AQI Prediction Dashboard")

# Display a brief introduction
st.markdown("""
This dashboard shows the AQI (Air Quality Index) predictions for the next 3 days based on weather and pollution data.
Click on the button below to fetch the latest AQI predictions.
""")

# Button to fetch the predictions
if st.button("Get AQI Predictions"):
    # Send GET request to FastAPI backend
    response = requests.get(API_URL)
    
    if response.status_code == 200:
        data = response.json()

        if "predictions" in data:
            # If predictions are available, display them
            predictions_df = pd.DataFrame(data["predictions"])

            st.subheader("AQI Predictions for the Next 3 Days")
            st.dataframe(predictions_df)

            # Optionally, show predictions on a chart
            st.subheader("AQI Predictions Chart")
            st.line_chart(predictions_df.set_index('day_offset')['predicted_aqi'])
        else:
            st.error("Error: No prediction data available.")
    else:
        st.error(f"Failed to fetch data. Status code: {response.status_code}")
