import functions_framework
import json
import logging
import requests
import csv
import zipfile
import polyline
from datetime import datetime
from zoneinfo import ZoneInfo
from io import BytesIO, StringIO
from typing import Optional, Dict, List, Any, Tuple
from collections import defaultdict
from firebase_admin import initialize_app, firestore
from google.cloud.logging import Client

# Initialize Firebase and logging
initialize_app()
db = firestore.client()
Client().setup_logging()

EASTERN = ZoneInfo("America/New_York")
GTFS_ZIP_URL = "https://passio3.com/ncstateuni/passioTransit/gtfs/google_transit.zip"
COLLECTION = "gtfsStatic"
REQUIRED_FILES = [
    "routes.txt",
    "trips.txt",
    "shapes.txt",
    "stops.txt",
    "stop_times.txt",
    "calendar.txt",
    "calendar_dates.txt",
    "feed_info.txt",
]


# ── Helpers ──────────────────────────────────────────────────────────


def time_to_seconds(t: str) -> int:
    """Convert HH:MM:SS to seconds since midnight. Handles times > 24:00."""
    h, m, s = t.strip().split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)


# ── Version Check ────────────────────────────────────────────────────


def should_update_feed(new_feed_info: Dict) -> bool:
    """Check if we need to update by comparing version and end date."""
    try:
        doc = db.collection(COLLECTION).document("schedule").get()
        if not doc.exists:
            logging.info("No existing schedule found — first run.")
            return True

        stored = doc.to_dict().get("feedInfo", {})
        stored_version = stored.get("version", "")
        stored_end_date = stored.get("endDate", "")
        new_version = new_feed_info.get("version", "")
        today = datetime.now().strftime("%Y%m%d")

        # New version published by PassIO
        if new_version != stored_version:
            logging.info(
                f"Feed version changed: {stored_version} → {new_version}"
            )
            return True

        # Current feed has expired
        if stored_end_date and today > stored_end_date:
            logging.info(
                f"Feed expired: endDate {stored_end_date}, today {today}"
            )
            return True

        logging.info(
            f"Feed unchanged (version: {stored_version}, "
            f"valid through: {stored_end_date}). Skipping update."
        )
        return False

    except Exception as e:
        logging.warning(f"Error checking feed version, updating anyway: {e}")
        return True


# ── GTFS Fetch & Extract ─────────────────────────────────────────────


def fetch_gtfs_zip() -> Optional[Dict[str, str]]:
    """Download and extract all required GTFS text files from the zip."""
    try:
        resp = requests.get(GTFS_ZIP_URL, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to fetch GTFS zip: {e}")
        return None

    try:
        file_contents = {}
        with zipfile.ZipFile(BytesIO(resp.content)) as zf:
            for filename in REQUIRED_FILES:
                with zf.open(filename) as f:
                    file_contents[filename] = f.read().decode("utf-8")
        return file_contents
    except Exception as e:
        logging.error(f"Failed to extract GTFS zip: {e}")
        return None


# ── Parsers ──────────────────────────────────────────────────────────


def parse_feed_info(content: str) -> Dict:
    """Parse feed_info.txt for feed validity window and version."""
    for row in csv.DictReader(StringIO(content)):
        return {
            "startDate": row.get("feed_start_date", "").strip(),
            "endDate": row.get("feed_end_date", "").strip(),
            "version": row.get("feed_version", "").strip(),
        }
    return {}


def parse_calendar(content: str) -> Dict[str, Dict]:
    """Parse calendar.txt into service_id -> day availability map."""
    services = {}
    for row in csv.DictReader(StringIO(content)):
        sid = row["service_id"].strip()
        services[sid] = {
            "days": {
                "monday": row["monday"] == "1",
                "tuesday": row["tuesday"] == "1",
                "wednesday": row["wednesday"] == "1",
                "thursday": row["thursday"] == "1",
                "friday": row["friday"] == "1",
                "saturday": row["saturday"] == "1",
                "sunday": row["sunday"] == "1",
            },
            "startDate": row["start_date"].strip(),
            "endDate": row["end_date"].strip(),
        }
    return services


def parse_calendar_dates(content: str) -> Dict[str, List[str]]:
    """Parse calendar_dates.txt. Returns service_id -> list of removed dates.

    All exceptions in this feed are type 2 (service removed), so we only
    need to track dates where service is NOT running.
    """
    removed = defaultdict(list)
    for row in csv.DictReader(StringIO(content)):
        if row["exception_type"].strip() == "2":
            removed[row["service_id"].strip()].append(row["date"].strip())
    return dict(removed)


def parse_routes(content: str) -> List[Dict]:
    """Parse routes.txt into a list of route dicts."""
    routes = []
    for row in csv.DictReader(StringIO(content)):
        routes.append({
            "id": row.get("route_id", "").strip(),
            "shortName": row.get("route_short_name", "").strip(),
            "longName": row.get("route_long_name", "").strip(),
            "color": row.get("route_color", "").strip(),
        })
    return routes


def parse_trips(content: str) -> Tuple[Dict[str, List[Dict]], Dict[str, Dict]]:
    """Parse trips.txt. Returns (trips_by_route, trips_map).

    trips_map: trip_id -> {routeID, serviceID, shapeID}
    trips_by_route: route_id -> [{tripID, serviceID, shapeID}, ...]
    """
    trips_by_route: Dict[str, List[Dict]] = defaultdict(list)
    trips_map: Dict[str, Dict] = {}

    for row in csv.DictReader(StringIO(content)):
        route_id = row.get("route_id", "").strip()
        trip_id = row.get("trip_id", "").strip()
        service_id = row.get("service_id", "").strip()
        shape_id = row.get("shape_id", "").strip()

        trip_info = {
            "tripID": trip_id,
            "routeID": route_id,
            "serviceID": service_id,
            "shapeID": shape_id,
        }

        trips_map[trip_id] = trip_info
        trips_by_route[route_id].append(trip_info)

    return dict(trips_by_route), trips_map


def parse_stops(content: str) -> Dict[str, Dict]:
    """Parse stops.txt into a dict keyed by stop_id for O(1) lookup."""
    stops = {}
    for row in csv.DictReader(StringIO(content)):
        stop_id = row.get("stop_id", "").strip()
        if not stop_id:
            continue
        try:
            lat = float(row.get("stop_lat", "0").strip())
            lng = float(row.get("stop_lon", "0").strip())
        except (ValueError, TypeError):
            lat, lng = 0.0, 0.0
        stops[stop_id] = {
            "code": row.get("stop_code", "").strip(),
            "name": row.get("stop_name", "").strip(),
            "coordinate": {
                "lat": lat,
                "lng": lng,
            },
        }
    return stops


def parse_stop_times(content: str) -> Dict[str, List[Dict]]:
    """Parse stop_times.txt into trip_id -> sorted list of stop entries."""
    by_trip: Dict[str, List[Dict]] = defaultdict(list)

    for row in csv.DictReader(StringIO(content)):
        by_trip[row["trip_id"].strip()].append({
            "seq": int(row["stop_sequence"]),
            "stopID": row["stop_id"].strip(),
            "arrival": row["arrival_time"].strip(),
        })

    for trip_id in by_trip:
        by_trip[trip_id].sort(key=lambda x: x["seq"])

    return dict(by_trip)


def parse_shapes(content: str, shape_to_route: Dict[str, str]) -> Dict[str, Dict]:
    """Parse shapes.txt and encode as polylines. Keyed by shapeID for O(1) lookup."""
    raw_shapes: Dict[str, List[Tuple[float, float]]] = defaultdict(list)

    for row in csv.DictReader(StringIO(content)):
        try:
            shape_id = row.get("shape_id", "").strip()
            lat = float(row.get("shape_pt_lat", "0").strip())
            lon = float(row.get("shape_pt_lon", "0").strip())
            raw_shapes[shape_id].append((lat, lon))
        except (ValueError, TypeError) as e:
            logging.warning(f"Skipped shape point: {e}")

    shapes = {}
    for shape_id, coords in raw_shapes.items():
        shapes[shape_id] = {
            "routeID": shape_to_route.get(shape_id, ""),
            "encodedPath": polyline.encode(coords),
        }
    return shapes


# ── Pattern Extraction ───────────────────────────────────────────────


def build_route_patterns(
    trips_by_route: Dict[str, List[Dict]],
    stop_times_by_trip: Dict[str, List[Dict]],
) -> Tuple[Dict[str, List[Dict]], Dict[str, str]]:
    """Extract unique stop/delta patterns per route.

    Instead of storing 1,094 full trip schedules, we reduce to ~26 patterns.
    Each pattern has the ordered stop list and inter-stop deltas (seconds).

    Returns:
        patterns_by_route: route_id -> list of pattern dicts
        trip_to_pattern: trip_id -> patternID
    """
    patterns_by_route: Dict[str, List[Dict]] = {}
    trip_to_pattern: Dict[str, str] = {}

    for route_id, trips in trips_by_route.items():
        seen: Dict[tuple, Dict] = {}
        pattern_idx = 0

        for trip_info in trips:
            trip_id = trip_info["tripID"]
            stops = stop_times_by_trip.get(trip_id)
            if not stops:
                continue

            # Build pattern key: ordered stops + inter-stop deltas
            stop_order = tuple(s["stopID"] for s in stops)
            deltas = tuple(
                time_to_seconds(stops[i]["arrival"])
                - time_to_seconds(stops[i - 1]["arrival"])
                for i in range(1, len(stops))
            )
            key = (stop_order, deltas)

            if key not in seen:
                pattern_id = f"{route_id}_p{pattern_idx}"
                pattern_idx += 1
                seen[key] = {
                    "patternID": pattern_id,
                    "stops": list(stop_order),
                    "deltas": list(deltas),
                    "tripIDs": [],
                }

            seen[key]["tripIDs"].append(trip_id)
            trip_to_pattern[trip_id] = seen[key]["patternID"]

        patterns_by_route[route_id] = list(seen.values())

    total = sum(len(p) for p in patterns_by_route.values())
    logging.info(f"Reduced {len(trip_to_pattern)} trips to {total} unique patterns")
    return patterns_by_route, trip_to_pattern


# ── Processing Pipeline ──────────────────────────────────────────────


def process_gtfs(file_contents: Dict[str, str]) -> Dict[str, Any]:
    """Run the full GTFS parsing pipeline."""

    # Core transit data
    routes = parse_routes(file_contents["routes.txt"])
    trips_by_route, trips_map = parse_trips(file_contents["trips.txt"])
    stops = parse_stops(file_contents["stops.txt"])
    stop_times_by_trip = parse_stop_times(file_contents["stop_times.txt"])

    # Calendar / scheduling data
    calendar = parse_calendar(file_contents["calendar.txt"])
    calendar_dates = parse_calendar_dates(file_contents["calendar_dates.txt"])
    feed_info = parse_feed_info(file_contents["feed_info.txt"])

    # Build pattern data
    patterns_by_route, trip_to_pattern = build_route_patterns(
        trips_by_route, stop_times_by_trip
    )

    # Enrich routes with pattern info and determine default shape
    routes_map: Dict[str, Dict] = {}
    shape_to_route: Dict[str, str] = {}

    for route in routes:
        route_id = route["id"]
        patterns = patterns_by_route.get(route_id, [])

        if not patterns:
            continue

        # Dominant pattern = most trips, used for default stop order
        dominant = max(patterns, key=lambda p: len(p["tripIDs"]))

        # Get shape from the first trip
        first_trip = trips_map.get(dominant["tripIDs"][0], {})
        shape_id = first_trip.get("shapeID", "")
        shape_to_route[shape_id] = route_id

        routes_map[route_id] = {
            **route,
            "shapeID": shape_id,
            "stops": dominant["stops"],
            "patterns": [
                {
                    "patternID": p["patternID"],
                    "stops": p["stops"],
                    "deltas": p["deltas"],
                    "tripCount": len(p["tripIDs"]),
                }
                for p in patterns
            ],
        }

    logging.info(f"Enriched {len(routes_map)} routes with patterns")

    # Parse shapes
    shapes = parse_shapes(file_contents["shapes.txt"], shape_to_route)

    return {
        "routes": routes_map,
        "stops": stops,
        "shapes": shapes,
        "tripLookup": trip_to_pattern,
        "tripsMap": trips_map,
        "calendar": calendar,
        "calendarDates": calendar_dates,
        "feedInfo": feed_info,
    }


# ── Firestore ────────────────────────────────────────────────────────


def save_to_firestore(data: Dict[str, Any], last_updated: str):
    """Save all transit data to Firestore.

    Documents:
      routes   — routeID -> route info + embedded patterns with stop order & deltas
      stops    — stopID -> stop name, code, coordinate
      shapes   — shapeID -> routeID + encoded polyline path
      trips    — tripID -> {routeID, serviceID, patternID}
      schedule — calendar, calendar_dates, feed_info
    """
    col = db.collection(COLLECTION)

    # Routes — keyed by routeID for O(1) lookup from trip resolution
    col.document("routes").set({
        "routes": data["routes"],
        "lastUpdated": last_updated,
    })
    logging.info(f"Saved {len(data['routes'])} routes with patterns")

    # Stops — keyed by stopID for O(1) lookup when resolving stop names/coordinates
    col.document("stops").set({
        "stops": data["stops"],
        "lastUpdated": last_updated,
    })
    logging.info(f"Saved {len(data['stops'])} stops")

    # Shapes — keyed by shapeID for O(1) lookup when drawing route paths
    col.document("shapes").set({
        "shapes": data["shapes"],
        "lastUpdated": last_updated,
    })
    logging.info(f"Saved {len(data['shapes'])} shapes")

    # Trips — tripID -> compact lookup for resolving live GTFS-RT trip IDs
    trip_lookup = {}
    for trip_id, trip_info in data["tripsMap"].items():
        trip_lookup[trip_id] = {
            "routeID": trip_info["routeID"],
            "serviceID": trip_info["serviceID"],
            "patternID": data["tripLookup"].get(trip_id, ""),
        }

    col.document("trips").set({
        "trips": trip_lookup,
        "tripCount": len(trip_lookup),
        "lastUpdated": last_updated,
    })
    logging.info(f"Saved {len(trip_lookup)} trip lookups")

    # Schedule — calendar + exceptions + feed version
    col.document("schedule").set({
        "calendar": data["calendar"],
        "calendarDates": data["calendarDates"],
        "feedInfo": data["feedInfo"],
        "lastUpdated": last_updated,
    })
    logging.info("Saved schedule data")


# ── Entry Point ──────────────────────────────────────────────────────


@functions_framework.http
def get_gtfs_static(request):
    """Cloud Function (HTTP): fetch GTFS static data, process, and save to Firestore.

    Checks feed version and end date before processing. Skips if unchanged.
    Scheduled via Cloud Scheduler (e.g. daily or every few hours).
    """
    try:
        last_updated = datetime.now(EASTERN).strftime("%Y-%m-%d %I:%M:%S %p")

        # Fetch zip to check feed version before full processing
        file_contents = fetch_gtfs_zip()
        if not file_contents:
            logging.error("Failed to fetch/extract GTFS zip — aborting.")
            return (json.dumps({"status": "error", "message": "Failed to fetch GTFS zip"}), 500, {"Content-Type": "application/json"})

        # Quick parse of feed_info to check version
        feed_info = parse_feed_info(file_contents["feed_info.txt"])
        if not should_update_feed(feed_info):
            return (json.dumps({"status": "ok", "message": "Feed unchanged — skipped update"}), 200, {"Content-Type": "application/json"})

        # Full processing pipeline
        data = process_gtfs(file_contents)
        save_to_firestore(data, last_updated)

        route_count = len(data["routes"])
        stop_count = len(data["stops"])
        shape_count = len(data["shapes"])
        trip_count = len(data["tripsMap"])

        logging.info("Wolfline GTFS data saved successfully.")
        return (json.dumps({
            "status": "ok",
            "routes": route_count,
            "stops": stop_count,
            "shapes": shape_count,
            "trips": trip_count,
        }), 200, {"Content-Type": "application/json"})

    except Exception as e:
        logging.exception(f"Unhandled error in get_gtfs_static: {e}")
        return (json.dumps({"status": "error", "message": str(e)}), 500, {"Content-Type": "application/json"})