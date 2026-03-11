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


def safe_float(value, default=0.0):
    """Safely parse a value to float."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value, default=0):
    """Safely parse a value to int."""
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def wind_direction_label(degrees):
    """Convert wind degrees to a cardinal/intercardinal direction."""
    directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                   "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    try:
        index = round(float(degrees) / 22.5) % 16
        return directions[index]
    except (ValueError, TypeError):
        return "N"


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
    temp = safe_float(temperature)
    if temp <= 50 and safe_float(wind_speed) > 3:
        return round(safe_float(wind_chill, temp))
    elif temp >= 80 and safe_float(humidity) > 40:
        return round(safe_float(heat_index, temp))
    return round(temp)


@functions_framework.http
def get_weather(request):
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
            return (json.dumps({"status": "error", "message": "Unexpected response format"}), 500, {"Content-Type": "application/json"})

        readings = extract_readings(station)
        image_url = extract_camera_url(station)

        temperature = readings.get("Thermometer")
        if not temperature:
            logging.error("Temperature not found in WeatherStem response.")
            return (json.dumps({"status": "error", "message": "No temperature data"}), 500, {"Content-Type": "application/json"})

        rounded_temp = round(safe_float(temperature))
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

        last_updated = datetime.now(EASTERN).strftime("%Y-%m-%d %I:%M:%S %p")

        weather_data = {
            "temperature": rounded_temp,
            "feelsLike": feels_like,
            "humidity": safe_int(readings.get("Hygrometer", 0)),
            "wind": {
                "speed": safe_int(readings.get("Anemometer", 0)),
                "gust": safe_int(readings.get("10 Minute Wind Gust", 0)),
                "direction": wind_direction_label(readings.get("Wind Vane", 0)),
                "degrees": safe_int(readings.get("Wind Vane", 0)),
            },
            "uvIndex": safe_int(readings.get("UV Radiation Sensor", 0)),
            "rain": {
                "rate": safe_float(readings.get("Rain Rate", "0.00")),
                "total": safe_float(readings.get("Rain Gauge", "0.00")),
            },
            "solarRadiation": safe_int(readings.get("Solar Radiation Sensor", 0)),
            "sunrise": sunrise,
            "sunset": sunset,
            "lastUpdated": last_updated,
        }

        if image_url:
            weather_data["imageUrl"] = image_url

        firebase_db.reference("weather").set(weather_data)
        logging.info(f"Weather updated: {rounded_temp}°F, feels like {feels_like}°F")
        return (json.dumps({"status": "ok", "temperature": rounded_temp, "feelsLike": feels_like}), 200, {"Content-Type": "application/json"})

    except requests.exceptions.Timeout:
        logging.error("WeatherStem API timed out")
        return (json.dumps({"status": "error", "message": "WeatherStem API timeout"}), 500, {"Content-Type": "application/json"})

    except requests.exceptions.RequestException as e:
        logging.error(f"WeatherStem API request failed: {e}")
        return (json.dumps({"status": "error", "message": str(e)}), 500, {"Content-Type": "application/json"})

    except Exception as e:
        logging.error(f"Unhandled error in get_weather: {e}")
        return (json.dumps({"status": "error", "message": str(e)}), 500, {"Content-Type": "application/json"})