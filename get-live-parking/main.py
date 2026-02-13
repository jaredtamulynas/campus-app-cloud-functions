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
    data = {
        "id": key,
        "name": lot.get("location_name", ""),
        "location": {
            "address": lot.get("location_address", "") or None,
            "coordinate": parse_coordinate(lot.get("geocode", "")),
        },
        "totalSpaces": parse_int(lot.get("total_spaces")),
        "availableSpaces": parse_int(lot.get("free_spaces")),
        "occupancy": parse_int(lot.get("occupancy")),
        "isHidden": current_data.get("isHidden", False),
    }

    return data


@functions_framework.cloud_event
def get_live_parking(cloud_event):
    try:
        response = fetch_parking_data()
        logging.info(f"OpenSpace response type: {type(response).__name__}")

        # The API returns a nested list — the actual lots are in the first element
        if isinstance(response, list) and len(response) > 0:
            parking_lots = response[0] if isinstance(response[0], list) else response
        else:
            logging.error(f"Unexpected response format: {json.dumps(response)[:500]}")
            return

        if not parking_lots:
            logging.info("No parking lots returned from API.")
            return

        ref = firebase_db.reference("liveParking")
        lots_ref = ref.child("lots")
        current_lots = lots_ref.get() or {}
        current_lot_keys = set(current_lots.keys())
        updated_lot_keys = set()

        for lot in parking_lots:
            name = lot.get("location_name")
            if not name:
                logging.warning(f"Skipping lot with no name: {lot}")
                continue

            key = lot_key_from_name(name)
            updated_lot_keys.add(key)

            current_data = current_lots.get(key, {})
            lot_data = build_lot_data(lot, current_data, key)
            lots_ref.child(key).update(lot_data)

        # Remove lots no longer returned by the API
        obsolete_keys = current_lot_keys - updated_lot_keys
        for key in obsolete_keys:
            lots_ref.child(key).delete()
            logging.info(f"Removed obsolete lot: {key}")

        # Update timestamp at the top level
        timestamp_str = datetime.now(EASTERN).strftime("%Y-%m-%d %I:%M:%S %p")
        ref.child("lastUpdated").set(timestamp_str)

        logging.info(f"Parking updated: {len(updated_lot_keys)} lots, {len(obsolete_keys)} removed")

    except Exception as e:
        logging.error(f"Unhandled error in get_live_parking: {e}")