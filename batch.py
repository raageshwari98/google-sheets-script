import gspread
import openmeteo_requests
import requests_cache
from requests.exceptions import RequestException
from datetime import datetime
import pytz
import time
import concurrent.futures
from retry_requests import retry

def get_weather(latitudes, longitudes):
    """
    Get current weather information for multiple locations.

    Args:
        latitudes (list): List of latitude values.
        longitudes (list): List of longitude values.

    Returns:
        list: A list of tuples, each containing the current temperature in Fahrenheit and wind speed in mph for a location.
    """
    try:
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
        retry_session = retry(cache_session, retries=3, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)

        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": latitudes,
            "longitude": longitudes,
            "current": ["temperature_2m", "wind_speed_10m"],
            "temperature_unit": TEMPERATURE_UNIT,
            "wind_speed_unit": WIND_SPEED_UNIT,
            "timezone": TIMEZONE,
            "forecast_days": FORECAST_DAYS,
            "forecast_minutely_15": FORECAST_MINUTELY_15
        }
        responses = openmeteo.weather_api(url, params=params)

        weather_data = []
        for response in responses:
            # Process each location's response
            current = response.Current()
            current_temperature = current.Variables(0).Value().__round__()
            current_wind_speed = "{:.1f}".format(current.Variables(1).Value())
            weather_data.append((current_temperature, current_wind_speed))

    except RequestException as e:
        # Handle network-related errors
        print(f"Error fetching weather data: {e}")
        return None

    except Exception as e:
        # Handle other types of errors
        print(f"Error: {e}")
        return None

    return weather_data

start_time = time.time()
# Constants
TEMPERATURE_UNIT = "fahrenheit"
WIND_SPEED_UNIT = "mph"
TIMEZONE = "America/Los_Angeles"
FORECAST_DAYS = 1
FORECAST_MINUTELY_15 = 4

# Import Google Sheets API credentials
service_account = gspread.service_account(filename="../acryldata-a4b1a908c233.json")

# Open a spreadsheet by its title
sheet = service_account.open("AcrylData_Exercise")

# Define the worksheet by its name
wks = sheet.worksheet("Sheet1")

# Manipulate the worksheet
lat_list = wks.col_values(1)[1:]
long_list = wks.col_values(2)[1:]

current_time_pst = datetime.now(pytz.timezone(TIMEZONE))
formatted_time = current_time_pst.strftime("%Y-%m-%d %I:%M %p")

# Batch the latitude and longitude pairs into chunks
batch_size = 10
latitudes_batches = [lat_list[i:i + batch_size] for i in range(0, len(lat_list), batch_size)]
longitudes_batches = [long_list[i:i + batch_size] for i in range(0, len(long_list), batch_size)]

for latitudes_batch, longitudes_batch in zip(latitudes_batches, longitudes_batches):
    # Fetch weather data for the current batch
    weather_data_batch = get_weather(latitudes_batch, longitudes_batch)

    # Update cells with weather data
    for k, (temp, wind_speed) in enumerate(weather_data_batch, 2):
        wks.update_cell(k, 3, f"{temp} F")
        wks.update_cell(k, 4, f"{wind_speed} mph")
        wks.update_cell(k, 5, formatted_time)

print("Google Sheet is updated.")

end_time = time.time()
runtime = end_time - start_time
print("Total Runtime:", runtime, "seconds")