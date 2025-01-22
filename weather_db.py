import openmeteo_requests
import requests_cache
import pandas as pd

from retry_requests import retry
from sqlalchemy import create_engine


class Weather:
    def __init__(self, url, params):
        self.url = url
        self.params = params
    
    def get_data(self):
        cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
        retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
        openmeteo = openmeteo_requests.Client(session = retry_session)

        responses = openmeteo.weather_api(self.url, params=self.params)

        # Process first location. Add a for-loop for multiple locations or weather models
        response = responses[0]
        print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
        print(f"Elevation {response.Elevation()} m asl")
        print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
        print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_rain = hourly.Variables(1).ValuesAsNumpy()
        hourly_snowfall = hourly.Variables(2).ValuesAsNumpy()
        hourly_wind_speed_10m = hourly.Variables(3).ValuesAsNumpy()
        hourly_wind_direction_10m = hourly.Variables(4).ValuesAsNumpy()

        hourly_data = {"date": pd.date_range(
            start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
            end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = hourly.Interval()),
            inclusive = "left"
        )}

        hourly_data["temperature"] = hourly_temperature_2m
        hourly_data["rain"] = hourly_rain
        hourly_data["snowfall"] = hourly_snowfall
        hourly_data["wind_speed"] = hourly_wind_speed_10m
        hourly_data["wind_direction"] = hourly_wind_direction_10m

        hourly_dataframe = pd.DataFrame(data = hourly_data)
        self.hourly_dataframe = hourly_dataframe
        return hourly_dataframe
    

    def SQLite(self, hourly_dataframe):
        engine = create_engine('sqlite:///mydb.db')
        hourly_dataframe.to_sql('my_table', engine, if_exists='replace')
        return engine

