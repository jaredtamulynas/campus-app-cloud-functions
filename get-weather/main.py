import functions_framework
import json
import logging
import requests
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from google.cloud.logging import Client
from firebase_admin import initialize_app, db as firebase_db
from astral import LocationInfo
from astral.sun import sun

# Initialize Firebase
initialize_app(options={'databaseURL': 'https://ot-campus-app-default-rtdb.firebaseio.com/'})

# Set up Cloud Logging
Client().setup_logging()

WEATHERSTEM_URL = "https://api.weatherstem.com/api"
NCSTATE_LAT = 35.7717255492
NCSTATE_LON = -78.6736536026
EASTERN = ZoneInfo("America/New_York")


def fetch_weatherstem_data():
    """Fetch weather data from the WeatherStem API."""
    payload = {
        "api_key": os.getenv("WEATHERSTEM_API_KEY", ""),
        "stations": ["ncstate@wake.weatherstem.com"],
    }
    response = requests.post(
        WEATHERSTEM_URL,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        data=json.dumps(payload),
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


def extract_readings(station):
    """Extract sensor readings into a flat dict keyed by sensor_type."""
    readings = station.get("record", {}).get("readings", [])
    return {r["sensor_type"]: r["value"] for r in readings}


def extract_camera_url(station):
    """Extract the Cloud Camera image URL."""
    for camera in station.get("station", {}).get("cameras", []):
        if camera.get("name") == "Cloud Camera":
            return camera.get("image")
    return None


def wind_direction_label(degrees):
    """Convert wind degrees to a cardinal/intercardinal direction."""
    directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                   "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    index = round(float(degrees) / 22.5) % 16
    return directions[index]


def get_sunrise_sunset():
    """Calculate sunrise/sunset for NC State's location today."""
    location = LocationInfo(latitude=NCSTATE_LAT, longitude=NCSTATE_LON)
    s = sun(location.observer, date=datetime.now(EASTERN).date(), tzinfo=EASTERN)
    return (
        int(s["sunrise"].timestamp()),
        int(s["sunset"].timestamp()),
    )


def determine_feels_like(temperature, wind_chill, heat_index, wind_speed, humidity):
    """Return the appropriate 'feels like' value based on NWS standards."""
    temp = float(temperature)
    if temp <= 50 and float(wind_speed) > 3:
        return round(float(wind_chill))
    elif temp >= 80 and float(humidity) > 40:
        return round(float(heat_index))
    return round(temp)


@functions_framework.cloud_event
def get_weather(cloud_event):
    try:
        data = fetch_weatherstem_data()
        logging.info(f"WeatherStem response type: {type(data).__name__}")

        # Safely extract the first station object
        if isinstance(data, list) and len(data) > 0:
            station = data[0]
        elif isinstance(data, dict) and "record" in data:
            station = data
        else:
            logging.error(f"Unexpected response format: {json.dumps(data)[:500]}")
            return

        readings = extract_readings(station)
        image_url = extract_camera_url(station)

        temperature = readings.get("Thermometer")
        if not temperature:
            logging.error("Temperature not found in WeatherStem response.")
            return

        rounded_temp = round(float(temperature))
        feels_like = determine_feels_like(
            temperature,
            readings.get("Wind Chill", temperature),
            readings.get("Heat Index", temperature),
            readings.get("Anemometer", 0),
            readings.get("Hygrometer", 0),
        )

        try:
            sunrise, sunset = get_sunrise_sunset()
        except Exception as e:
            logging.warning(f"Failed to calculate sunrise/sunset: {e}")
            sunrise, sunset = None, None

        local_time = datetime.now(EASTERN)
        timestamp_str = local_time.strftime("%Y-%m-%d %I:%M:%S %p")

        weather_data = {
            "temperature": rounded_temp,
            "feelsLike": feels_like,
            "humidity": int(readings.get("Hygrometer", 0)),
            "wind": {
                "speed": int(readings.get("Anemometer", 0)),
                "gust": int(readings.get("10 Minute Wind Gust", 0)),
                "direction": wind_direction_label(readings.get("Wind Vane", 0)),
                "degrees": int(float(readings.get("Wind Vane", 0))),
            },
            "uvIndex": int(readings.get("UV Radiation Sensor", 0)),
            "rain": {
                "rate": float(readings.get("Rain Rate", "0.00")),
                "total": float(readings.get("Rain Gauge", "0.00")),
            },
            "solarRadiation": int(readings.get("Solar Radiation Sensor", 0)),
            "sunrise": sunrise,
            "sunset": sunset,
            "lastUpdated": timestamp_str,
        }

        if image_url:
            weather_data["imageUrl"] = image_url

        firebase_db.reference("weather").set(weather_data)
        logging.info(f"Weather updated: {rounded_temp}°F, feels like {feels_like}°F")

    except Exception as e:
        logging.error(f"Unhandled error in get_weather: {e}")