from fastapi import FastAPI, Query
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry

# Setup Open-Meteo API client with cache + retries
cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

app = FastAPI(title="Rainfall API")

@app.get("/annual-rainfall")
def get_annual_rainfall(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
):
    # Fixed custom date range: Sep 13, 2024 → Sep 12, 2025
    start_date = "2024-09-13"
    end_date = "2025-09-12"

    # API call to Open-Meteo
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "precipitation_sum",
        "timezone": "UTC",
    }

    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    # Extract daily precipitation
    daily = response.Daily()
    daily_precip = daily.Variables(0).ValuesAsNumpy()

    # Build dataframe
    daily_data = {
        "date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s", utc=True),
            end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left",
        ),
        "precipitation_sum": daily_precip,
    }
    df = pd.DataFrame(daily_data)

    # Calculate total rainfall
    total_rainfall = df["precipitation_sum"].sum()

    return {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "rainfall_mm": round(float(total_rainfall), 2),
    }