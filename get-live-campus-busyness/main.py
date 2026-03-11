import functions_framework
import json
import logging
import time
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from google.cloud.logging import Client
from firebase_admin import initialize_app, db as firebase_db

# Initialize Firebase
initialize_app(options={'databaseURL': 'https://ot-campus-app-default-rtdb.firebaseio.com/'})

# Set up Cloud Logging
Client().setup_logging()

WAITZ_URL = "https://waitz.io/live/ncsu"
EASTERN = ZoneInfo("America/New_York")


def fetch_waitz_data():
    """Fetch busyness data from the Waitz API."""
    response = requests.get(WAITZ_URL, timeout=10)
    response.raise_for_status()
    return response.json()


def occupancy_status(occupancy):
    """Convert an occupancy percentage (0-100) to a status string."""
    if occupancy >= 80:
        return "veryHigh"
    elif occupancy >= 50:
        return "high"
    elif occupancy >= 25:
        return "moderate"
    return "low"


def build_sublocation(subloc):
    """Build the data dict for a single sublocation."""
    occupancy = subloc.get("busyness", 0)
    return {
        "id": subloc.get("id"),
        "name": subloc.get("name", ""),
        "occupancy": occupancy,
        "capacity": subloc.get("capacity", 0),
        "isOpen": subloc.get("isOpen", False),
        "status": occupancy_status(occupancy),
    }


def build_location(location):
    """Build the data dict for a single location."""
    sublocs = location.get("subLocs", [])
    occupancy = location.get("busyness", 0)

    # Find the least busy open sublocation
    best_spot = None
    best_locations = location.get("bestLocations", [])
    if best_locations and sublocs:
        best_id = best_locations[0].get("id")
        for s in sublocs:
            if s.get("id") == best_id:
                best_spot = s.get("name")
                break

    return {
        "id": location.get("id"),
        "name": location.get("name", ""),
        "occupancy": occupancy,
        "capacity": location.get("capacity", 0),
        "isOpen": location.get("isOpen", False),
        "status": occupancy_status(occupancy),
        "bestSpot": best_spot,
        "subLocations": [build_sublocation(s) for s in sublocs],
    }


@functions_framework.http
def get_live_campus_busyness(request):
    start_total = time.time()
    try:
        # --- Fetch ---
        start = time.time()
        data = fetch_waitz_data()
        logging.info(f"Waitz API: {time.time() - start:.2f}s — type: {type(data).__name__}")

        # --- Parse ---
        start = time.time()
        if isinstance(data, dict):
            locations = data.get("data", [])
        elif isinstance(data, list):
            locations = data
        else:
            logging.error(f"Unexpected response format: {json.dumps(data)[:500]}")
            return (json.dumps({"status": "error", "message": "Unexpected response format"}), 500, {"Content-Type": "application/json"})

        if not locations:
            logging.info("No locations returned from Waitz.")
            return (json.dumps({"status": "ok", "message": "No locations returned"}), 200, {"Content-Type": "application/json"})

        location_updates = {}
        for location in locations:
            loc_id = location.get("id")
            if not loc_id:
                logging.warning(f"Skipping location with no id: {location.get('name')}")
                continue
            location_updates[str(loc_id)] = build_location(location)

        logging.info(f"Parse: {time.time() - start:.2f}s — {len(location_updates)} locations")

        # --- Write locations ---
        start = time.time()
        ref = firebase_db.reference("liveCampusBusyness")
        ref.child("locations").set(location_updates)
        logging.info(f"Firebase write locations: {time.time() - start:.2f}s")

        # --- Write lastUpdated ---
        start = time.time()
        last_updated = datetime.now(EASTERN).strftime("%Y-%m-%d %I:%M:%S %p")
        ref.child("lastUpdated").set(last_updated)
        logging.info(f"Firebase write lastUpdated: {time.time() - start:.2f}s")

        logging.info(f"Total: {time.time() - start_total:.2f}s — {len(location_updates)} locations updated")
        return (json.dumps({"status": "ok", "locations": len(location_updates)}), 200, {"Content-Type": "application/json"})

    except requests.exceptions.Timeout:
        logging.error(f"Waitz API timed out after {time.time() - start_total:.2f}s")
        return (json.dumps({"status": "error", "message": "Waitz API timeout"}), 500, {"Content-Type": "application/json"})

    except requests.exceptions.RequestException as e:
        logging.error(f"Waitz API request failed after {time.time() - start_total:.2f}s: {e}")
        return (json.dumps({"status": "error", "message": str(e)}), 500, {"Content-Type": "application/json"})

    except Exception as e:
        logging.error(f"Unhandled error after {time.time() - start_total:.2f}s: {e}")
        return (json.dumps({"status": "error", "message": str(e)}), 500, {"Content-Type": "application/json"})