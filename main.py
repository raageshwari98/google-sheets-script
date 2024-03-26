import gspread
import openmeteo_requests
import requests_cache
from requests.exceptions import RequestException
from datetime import datetime
import pytz
import time
import concurrent.futures
from retry_requests import retry


def get_weather(lat,long):
	"""
		Get current weather information for a given latitude and longitude.

		Args:
			lat (float): Latitude of the location.
			long (float): Longitude of the location.

			Returns:
				tuple: A tuple containing the current temperature in Fahrenheit and wind speed in mph.
	"""
	try:
		#Setup the Open-Meteo API client with cache and retry on error
		cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
		retry_session = retry(cache_session, retries = 3, backoff_factor = 0.2)
		openmeteo = openmeteo_requests.Client(session = retry_session)


		url = "https://api.open-meteo.com/v1/forecast"
		params = {
			"latitude": lat,
			"longitude": long,
			"current": ["temperature_2m", "wind_speed_10m"],
			"temperature_unit": TEMPERATURE_UNIT,
			"wind_speed_unit": WIND_SPEED_UNIT,
			"timezone": TIMEZONE,
			"forecast_days": FORECAST_DAYS,
			"forecast_minutely_15": FORECAST_MINUTELY_15
			}
		responses = openmeteo.weather_api(url, params=params)

		# Process first location.
		response = responses[0]

		# Current values.
		current = response.Current()
		current_temperature = current.Variables(0).Value().__round__()
		current_wind_speed = "{:.1f}".format(current.Variables(1).Value())

	except RequestException as e:
		# Handle network-related errors
		print(f"Error fetching weather data: {e}")
		return None, None

	except IndexError:
		# Handle IndexError if responses list is empty
		print("Error: No response received from the API")
		return None, None

	except Exception as e:
		# Handle other types of errors
		print(f"Error: {e}")
		return None, None

	return current_temperature,current_wind_speed



start_time = time.time()

# import Google Sheets API credentials
service_account = gspread.service_account(filename="../acryldata-a4b1a908c233.json")

# open a spreadsheet by its title
sheet = service_account.open("AcrylData_Exercise")

# define the worksheet by its name
wks = sheet.worksheet("Sheet1")

#constants
TEMPERATURE_UNIT = "fahrenheit"
WIND_SPEED_UNIT = "mph"
TIMEZONE = "America/Los_Angeles"
FORECAST_DAYS = 1
FORECAST_MINUTELY_15 = 4

# manipulate the worksheet
lat_list = wks.col_values(1)[1:]
long_list= wks.col_values(2)[1:]

current_time_pst = datetime.now(pytz.timezone(TIMEZONE))
formatted_time = current_time_pst.strftime("%Y-%m-%d %I:%M %p")


for k,(i, j) in enumerate( zip(lat_list, long_list),2):
	temp = get_weather(i, j)
	wks.update_cell(k,3,f"{temp[0]} F")
	wks.update_cell(k,4,f"{temp[1]} mph")
	wks.update_cell(k,5,formatted_time)

print("Google sheet is updated.")

end_time = time.time()
runtime = end_time - start_time
print("Total Runtime:", runtime, "seconds")



## Parallelism approach
# function to fetch weather data in parallel
"""def fetch_weather_parallel(lat_long):
    lat, long = lat_long
    return mateo(lat, long)
# Combine latitude and longitude data for parallel processing
lat_long_list = list(zip(lat_list, long_list))

# Use ThreadPoolExecutor for parallel execution
with concurrent.futures.ThreadPoolExecutor() as executor:
    # Fetch weather data in parallel
    results = executor.map(fetch_weather_parallel, lat_long_list)

# Update Google Sheets with the fetched weather data
for k, (temperature, wind_speed) in enumerate(results, start=2):
    wks.update_cell(k, 3, f"{temperature} F")
    wks.update_cell(k, 4, f"{wind_speed} mph")
    wks.update_cell(k, 5, formatted_time)"""
