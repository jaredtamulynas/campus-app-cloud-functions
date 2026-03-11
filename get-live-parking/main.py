import functions_framework
import json
import logging
import requests
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from google.cloud.logging import Client
from firebase_admin import initialize_app, db as firebase_db

# Initialize Firebase
initialize_app(options={'databaseURL': 'https://ot-campus-app-default-rtdb.firebaseio.com/'})

# Set up Cloud Logging
Client().setup_logging()

OPENSPACE_URL = "https://api.streetsoncloud.com/pl2/multi-lot-info"
EASTERN = ZoneInfo("America/New_York")


def fetch_parking_data():
    """Fetch parking data from the OpenSpace API."""
    response = requests.get(
        OPENSPACE_URL,
        headers={
            "Content-Type": "application/json",
            "x-api-key": os.getenv("OPENSPACE_API_KEY", ""),
        },
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


def parse_coordinate(geocode_str):
    """Parse a '(lat, lng)' string into a coordinate dict."""
    try:
        lat_str, lng_str = geocode_str.strip("()").split(",")
        return {"lat": float(lat_str), "lng": float(lng_str)}
    except (ValueError, AttributeError):
        return None


def parse_int(value, default=0):
    """Safely parse a value to int."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def lot_key_from_name(name):
    """Generate a camelCase Firebase key from a lot name."""
    words = name.split()
    return words[0].lower() + "".join(w.capitalize() for w in words[1:])


def build_lot_data(lot, current_data, key):
    """Build the update dict for a single parking lot."""
    return {
        "id": key,
        "name": lot.get("location_name", ""),
        "address": lot.get("location_address", "") or None,
        "coordinate": parse_coordinate(lot.get("geocode", "")),
        "totalSpaces": parse_int(lot.get("total_spaces")),
        "availableSpaces": parse_int(lot.get("free_spaces")),
        "occupancy": parse_int(lot.get("occupancy")),
        "isHidden": current_data.get("isHidden", False),
    }


@functions_framework.http
def get_live_parking(request):
    try:
        response = fetch_parking_data()
        logging.info(f"OpenSpace response type: {type(response).__name__}")

        # The API returns a nested list — the actual lots are in the first element
        if isinstance(response, list) and len(response) > 0:
            parking_lots = response[0] if isinstance(response[0], list) else response
        else:
            logging.error(f"Unexpected response format: {json.dumps(response)[:500]}")
            return (json.dumps({"status": "error", "message": "Unexpected response format"}), 500, {"Content-Type": "application/json"})

        if not parking_lots:
            logging.info("No parking lots returned from API.")
            return (json.dumps({"status": "ok", "message": "No parking lots returned"}), 200, {"Content-Type": "application/json"})

        ref = firebase_db.reference("liveParking")
        current_lots = ref.child("lots").get() or {}

        # Build all lot data in one pass
        updated_lots = {}
        for lot in parking_lots:
            name = lot.get("location_name")
            if not name:
                logging.warning(f"Skipping lot with no name: {lot}")
                continue

            key = lot_key_from_name(name)
            current_data = current_lots.get(key, {})
            updated_lots[key] = build_lot_data(lot, current_data, key)

        # Single atomic write replaces all lots and removes obsolete ones
        last_updated = datetime.now(EASTERN).strftime("%Y-%m-%d %I:%M:%S %p")
        ref.set({
            "lots": updated_lots,
            "lastUpdated": last_updated,
        })

        logging.info(f"Parking updated: {len(updated_lots)} lots")
        return (json.dumps({"status": "ok", "lots": len(updated_lots)}), 200, {"Content-Type": "application/json"})

    except requests.exceptions.Timeout:
        logging.error("OpenSpace API timed out")
        return (json.dumps({"status": "error", "message": "OpenSpace API timeout"}), 500, {"Content-Type": "application/json"})

    except requests.exceptions.RequestException as e:
        logging.error(f"OpenSpace API request failed: {e}")
        return (json.dumps({"status": "error", "message": str(e)}), 500, {"Content-Type": "application/json"})

    except Exception as e:
        logging.error(f"Unhandled error in get_live_parking: {e}")
        return (json.dumps({"status": "error", "message": str(e)}), 500, {"Content-Type": "application/json"})