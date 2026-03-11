import functions_framework
import json
import logging
import time
import requests
import polyline
from datetime import datetime
from zoneinfo import ZoneInfo
from google.cloud.logging import Client
from firebase_admin import initialize_app, firestore

# Initialize Firebase
initialize_app()
db = firestore.client()

# Set up Cloud Logging
Client().setup_logging()

EASTERN = ZoneInfo("America/New_York")

# GIS API URLs
BUILDINGS_URL = "https://gismaps.oit.ncsu.edu/arcgis/rest/services/Buildings/Buildings_OnlineCampusMap/MapServer/1/query?where=CITY%3D'Raleigh'&outFields=BLDG_NUM,BLDG_NAME,BLDG_ABBR,ADDRESS,CITY,STATE,ZIP,LATITUDE,LONGITUDE,MAPNAME&returnGeometry=false&returnTrueCurves=false&returnDistinctValues=true&f=pjson"

PARKING_LOTS_URL = "https://gismaps.oit.ncsu.edu/arcgis/rest/services/Transportation/Transportation_OnlineCampusMap/MapServer/1/query?where=1%3D1&outFields=OBJECTID,NAME,PRECINCT,SPACES,STATUS,ZONE_,Type&returnGeometry=true&outSR=4326&f=json"

ADA_PARKING_URL = "https://gismaps.oit.ncsu.edu/arcgis/rest/services/Transportation/Transportation_AccessibilityParkingSpaces/MapServer/0/query?where=1%3D1&outFields=OBJECTID_1,Campus,Location,Lat,Long&f=geojson"

ENTRANCES_URL = "https://gismaps.oit.ncsu.edu/arcgis/rest/services/Accessibility/Accessibility/MapServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json"

PATHS_BASE_URL = "https://gismaps.oit.ncsu.edu/arcgis/rest/services/Accessibility/Accessibility/MapServer/1/query"


def fetch_json(url):
    """Fetch JSON from a URL with timeout and error handling."""
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    return response.json()


def fetch_features(url, key="features"):
    """Fetch and return the features array from a GIS API response."""
    data = fetch_json(url)
    if key not in data:
        raise ValueError(f"No '{key}' key in response from {url}")
    return data[key]


# --- Buildings ---

def parse_building(item):
    a = item["attributes"]
    name = a.get("MAPNAME", "").strip()
    if not name:
        name = a.get("BLDG_NAME", "")

    return {
        "id": a["BLDG_NUM"],
        "name": name,
        "abbr": a["BLDG_ABBR"],
        "address": a["ADDRESS"],
        "city": a["CITY"],
        "state": a["STATE"],
        "zip": a["ZIP"],
        "coordinate": {
            "lat": a["LATITUDE"],
            "lng": a["LONGITUDE"],
        },
    }


def sync_buildings(last_updated):
    features = fetch_features(BUILDINGS_URL)
    buildings = []
    for item in features:
        try:
            buildings.append(parse_building(item))
        except (KeyError, TypeError) as e:
            logging.warning(f"Skipped building: {e}")

    if not buildings:
        logging.error("No buildings parsed.")
        return 0

    db.collection("gis").document("buildings").set({
        "buildings": buildings,
        "lastUpdated": last_updated,
    })
    return len(buildings)


# --- Parking Lots ---

def parse_parking_lot(item):
    a = item["attributes"]
    rings = item["geometry"]["rings"]
    encoded_rings = [
        polyline.encode([(lat, lng) for lng, lat in ring])
        for ring in rings
    ]

    return {
        "id": a["OBJECTID"],
        "name": a["NAME"],
        "precinct": a["PRECINCT"],
        "spaces": a["SPACES"],
        "status": a["STATUS"],
        "zone": a["ZONE_"],
        "type": a["Type"],
        "encodedGeometry": encoded_rings,
    }


def sync_parking_lots(last_updated):
    features = fetch_features(PARKING_LOTS_URL)
    lots = []
    for item in features:
        try:
            lots.append(parse_parking_lot(item))
        except (KeyError, TypeError) as e:
            logging.warning(f"Skipped parking lot: {e}")

    if not lots:
        logging.error("No parking lots parsed.")
        return 0

    db.collection("gis").document("parkingLots").set({
        "parkingLots": lots,
        "lastUpdated": last_updated,
    })
    return len(lots)


# --- ADA Parking ---

def parse_ada_spot(feature):
    return {
        "id": str(feature["id"]),
        "coordinate": {
            "lat": feature["geometry"]["coordinates"][1],
            "lng": feature["geometry"]["coordinates"][0],
        },
        "campus": feature["properties"]["Campus"],
        "locationType": feature["properties"]["Location"],
    }


def sync_ada_parking(last_updated):
    features = fetch_features(ADA_PARKING_URL)
    spots = []
    for item in features:
        try:
            if "geometry" in item and "coordinates" in item.get("geometry", {}):
                spots.append(parse_ada_spot(item))
        except (KeyError, TypeError) as e:
            logging.warning(f"Skipped ADA spot: {e}")

    if not spots:
        logging.error("No ADA parking spots parsed.")
        return 0

    db.collection("gis").document("adaParkingSpots").set({
        "adaParkingSpots": spots,
        "lastUpdated": last_updated,
    })
    return len(spots)


# --- Accessible Entrances ---

def parse_entrance(feature):
    a = feature["attributes"]
    g = feature["geometry"]

    return {
        "id": str(a["OBJECTID"]),
        "type": a["Type"],
        "description": a["Desc_"],
        "enabled": bool(a["Enabled"]),
        "doorOpener": (a.get("DoorOpener") or "").strip().lower() == "yes",
        "coordinate": {
            "lat": g["y"],
            "lng": g["x"],
        },
    }


def sync_entrances(last_updated):
    features = fetch_features(ENTRANCES_URL)
    entrances = []
    for item in features:
        try:
            a = item.get("attributes", {})
            g = item.get("geometry", {})
            if a.get("OBJECTID") is not None and g.get("x") is not None and g.get("y") is not None:
                entrances.append(parse_entrance(item))
        except (KeyError, TypeError) as e:
            logging.warning(f"Skipped entrance: {e}")

    if not entrances:
        logging.error("No accessible entrances parsed.")
        return 0

    db.collection("gis").document("accessibleFacilityEntrances").set({
        "accessibleFacilityEntrances": entrances,
        "lastUpdated": last_updated,
    })
    return len(entrances)


# --- Accessible Paths ---

def combine_paths(features, timeout_seconds=300, tolerance=0.0000165):
    start_time = time.time()
    original_count = len(features)
    used_indices = set()

    def coordinates_match(coord1, coord2):
        return abs(coord1[0] - coord2[0]) <= tolerance and abs(coord1[1] - coord2[1]) <= tolerance

    def merge_paths(path1, path2):
        if coordinates_match(path1[-1], path2[0]):
            return path1 + path2[1:]
        elif coordinates_match(path1[0], path2[-1]):
            return path2 + path1[1:]
        elif coordinates_match(path1[0], path2[0]):
            return list(reversed(path2)) + path1[1:]
        elif coordinates_match(path1[-1], path2[-1]):
            return path1 + list(reversed(path2[:-1]))
        return None

    combined_features = []
    timed_out = False

    for i in range(len(features)):
        if i in used_indices or "geometry" not in features[i] or "paths" not in features[i]["geometry"]:
            continue

        current_feature = features[i]
        current_path = current_feature["geometry"]["paths"][0]
        current_id = current_feature["attributes"]["OBJECTID"]
        current_slope = current_feature["attributes"]["Slope"]

        for j in range(i + 1, len(features)):
            if j in used_indices or "geometry" not in features[j] or "paths" not in features[j]["geometry"]:
                continue

            next_feature = features[j]
            next_slope = next_feature["attributes"]["Slope"]

            if current_slope != next_slope:
                continue

            merged_path = merge_paths(current_path, next_feature["geometry"]["paths"][0])
            if merged_path:
                current_path = merged_path
                used_indices.add(j)

        combined_features.append({
            "attributes": {"OBJECTID": current_id, "Slope": current_slope},
            "geometry": {"paths": [current_path]},
        })
        used_indices.add(i)

        if time.time() - start_time > timeout_seconds:
            timed_out = True
            logging.warning("Path combination timeout. Returning partial results.")
            break

    if timed_out:
        skipped = original_count - len(used_indices)
        logging.warning(f"Timeout: {skipped} path features were not processed.")

    logging.info(f"Paths reduced from {original_count} to {len(combined_features)}")
    return combined_features


def sync_accessible_paths(last_updated):
    all_features = []
    offset = 0

    for _ in range(10):
        url = (
            f"{PATHS_BASE_URL}?where=1%3D1&outFields=OBJECTID,Slope"
            f"&returnGeometry=true&f=json&outSR=4326"
            f"&resultRecordCount=1000&resultOffset={offset}"
        )
        data = fetch_json(url)

        if "features" not in data:
            break

        all_features.extend(data["features"])

        if data.get("exceededTransferLimit"):
            offset += 1000
        else:
            break

    logging.info(f"Total path features fetched: {len(all_features)}")

    if not all_features:
        logging.error("No accessible path features fetched.")
        return 0

    combined = combine_paths(all_features)

    paths = []
    for item in combined:
        try:
            geometry = item.get("geometry")
            if not geometry or "paths" not in geometry:
                continue
            encoded = polyline.encode(
                [(lat, lng) for lng, lat in geometry["paths"][0]]
            )
            paths.append({
                "id": item["attributes"]["OBJECTID"],
                "slope": item["attributes"]["Slope"],
                "encodedPath": encoded,
            })
        except (KeyError, TypeError, IndexError) as e:
            logging.warning(f"Skipped path: {e}")

    if not paths:
        logging.error("No accessible paths parsed.")
        return 0

    db.collection("gis").document("accessiblePaths").set({
        "accessiblePaths": paths,
        "lastUpdated": last_updated,
    })
    return len(paths)


# --- Main Entry Point ---

@functions_framework.http
def get_gis_data(request):
    """HTTP-triggered function that fetches all GIS data and writes to Firestore."""
    try:
        last_updated = datetime.now(EASTERN).strftime("%Y-%m-%d %I:%M:%S %p")

        buildings_count = sync_buildings(last_updated)
        lots_count = sync_parking_lots(last_updated)
        ada_count = sync_ada_parking(last_updated)
        entrances_count = sync_entrances(last_updated)
        paths_count = sync_accessible_paths(last_updated)

        logging.info(
            f"GIS sync complete — buildings: {buildings_count}, "
            f"parkingLots: {lots_count}, adaParking: {ada_count}, "
            f"entrances: {entrances_count}, paths: {paths_count} "
            f"({last_updated})"
        )
        return (json.dumps({
            "status": "ok",
            "buildings": buildings_count,
            "parkingLots": lots_count,
            "adaParking": ada_count,
            "entrances": entrances_count,
            "paths": paths_count,
        }), 200, {"Content-Type": "application/json"})

    except requests.exceptions.Timeout as e:
        logging.error(f"GIS API timed out: {e}")
        return (json.dumps({"status": "error", "message": f"GIS API timed out: {e}"}), 500, {"Content-Type": "application/json"})

    except requests.exceptions.RequestException as e:
        logging.error(f"GIS API request failed: {e}")
        return (json.dumps({"status": "error", "message": f"GIS API request failed: {e}"}), 500, {"Content-Type": "application/json"})

    except ValueError as e:
        logging.error(f"GIS API response invalid: {e}")
        return (json.dumps({"status": "error", "message": f"GIS API response invalid: {e}"}), 500, {"Content-Type": "application/json"})

    except Exception as e:
        logging.error(f"Unhandled error in get_gis_data: {e}")
        return (json.dumps({"status": "error", "message": str(e)}), 500, {"Content-Type": "application/json"})