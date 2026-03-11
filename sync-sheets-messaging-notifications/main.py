import functions_framework
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from google.cloud.logging import Client
from firebase_admin import initialize_app, db as firebase_db
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import google.auth.exceptions

# Initialize Firebase
initialize_app(options={"databaseURL": "https://ot-campus-app-default-rtdb.firebaseio.com/"})

# Set up Cloud Logging
Client().setup_logging()

EASTERN = ZoneInfo("America/New_York")

# Google Sheets config
SPREADSHEET_ID = "13LLOq7lc57nedozhP0GMthKQczUQktQZdvV6clZuFP8"
BANNERS_RANGE = "Banner Message!A26:I"
WELCOME_RANGE = "Welcome Message and Featured Image!A32:I"
GEO_RANGE = "Location Based - Notifications and Messaging!A34:N"

# Expected column headers (lowercase for comparison)
BANNER_HEADERS = ["label", "start date", "end date", "message", "optional link", "status", "approval", "group", "contact"]
WELCOME_HEADERS = ["label", "start date", "end date", "message", "image path", "status", "approval", "group", "contact"]
GEO_HEADERS = ["label", "start date", "end date", "location", "latitude", "longitude", "radius (m)", "title", "message", "image path", "status", "approval", "group", "contact"]


def get_sheets_service():
    """Build and return a Google Sheets API service using default credentials."""
    from google.auth import default
    creds, _ = default(scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def fetch_sheet_data(service, range_name):
    """Fetch rows from a Google Sheets range."""
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=SPREADSHEET_ID, range=range_name)
        .execute()
    )
    return result.get("values", [])


def validate_headers(rows, expected_headers, section_name):
    """Validate that the first row matches expected column headers.
    Returns True if valid, False if mismatch.
    """
    if not rows:
        logging.error(f"{section_name}: No rows returned")
        return False

    actual = [cell.strip().lower() for cell in rows[0]]

    if actual != expected_headers:
        logging.error(
            f"{section_name}: Column mismatch — "
            f"expected {expected_headers}, got {actual}"
        )
        return False

    return True


def safe_get(row, idx, default=""):
    """Safely get a value from a row by index."""
    return row[idx].strip() if idx < len(row) and row[idx] else default


def parse_date(date_str):
    """Parse a date string in M/D/YYYY or YYYY-MM-DD format."""
    if not date_str:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    return None


def slugify(text):
    """Convert text to a URL-safe key."""
    return text.strip().lower().replace(" ", "-").replace("/", "-").replace("&", "and")


def find_active_banner(rows, today):
    """Find the active banner for today.
    Columns: Label(0), Start Date(1), End Date(2), Message(3), Optional Link(4), Status(5), Approval(6)
    Newest (lowest in sheet) approved row with today in date range wins.
    """
    active = None

    for row in rows:
        if len(row) < 4:
            continue

        status = safe_get(row, 5).lower()
        approved = safe_get(row, 6).lower()

        if status != "final" or approved not in ("yes", "true"):
            continue

        start = parse_date(safe_get(row, 1))
        end = parse_date(safe_get(row, 2))

        if not start or not end:
            continue

        if start <= today <= end:
            entry = {
                "message": safe_get(row, 3),
                "startDate": start.isoformat(),
                "endDate": end.isoformat(),
            }
            link = safe_get(row, 4)
            if link:
                entry["link"] = link
            active = entry

    return active


def find_active_welcome(rows, today):
    """Find the active welcome message for today.
    Columns: Label(0), Start Date(1), End Date(2), Message(3), Image Path(4), Status(5), Approval(6)
    Newest (lowest in sheet) approved row with today in date range wins.
    """
    active = None

    for row in rows:
        if len(row) < 4:
            continue

        status = safe_get(row, 5).lower()
        approved = safe_get(row, 6).lower()

        if status != "final" or approved not in ("yes", "true"):
            continue

        start = parse_date(safe_get(row, 1))
        end = parse_date(safe_get(row, 2))

        if not start or not end:
            continue

        if start <= today <= end:
            entry = {
                "welcomeMessage": safe_get(row, 3),
                "startDate": start.isoformat(),
                "endDate": end.isoformat(),
            }
            image = safe_get(row, 4)
            if image:
                entry["imagePath"] = image
            active = entry

    return active


def get_approved_geofences(rows):
    """Get all approved geofence locations with their date ranges.
    Columns: Label(0), Start Date(1), End Date(2), Location(3), Latitude(4), Longitude(5),
             Radius(6), Title(7), Message(8), Image Path(9), Status(10), Approval(11)
    """
    locations = {}
    for row in rows:
        if len(row) < 9:
            continue

        status = safe_get(row, 10).lower()
        approved = safe_get(row, 11).lower()

        if status != "final" or approved not in ("yes", "true"):
            continue

        start = parse_date(safe_get(row, 1))
        end = parse_date(safe_get(row, 2))

        if not start or not end:
            continue

        label = safe_get(row, 0)
        if not label:
            continue

        key = slugify(label)

        try:
            lat = float(safe_get(row, 4))
            lng = float(safe_get(row, 5))
            radius = int(safe_get(row, 6, "150"))
        except (ValueError, TypeError):
            continue

        entry = {
            "label": label,
            "startDate": start.isoformat(),
            "endDate": end.isoformat(),
            "location": safe_get(row, 3),
            "coordinate": {
                "lat": lat,
                "lng": lng,
            },
            "radius": radius,
            "title": safe_get(row, 7),
            "message": safe_get(row, 8),
        }
        image = safe_get(row, 9)
        if image:
            entry["imagePath"] = image

        locations[key] = entry

    return locations


@functions_framework.http
def sync_sheet(request):
    """HTTP-triggered function that reads banner, welcome message, and geofence data
    from Google Sheets and writes them to Realtime Database."""
    try:
        service = get_sheets_service()
        today = datetime.now(EASTERN).date()
        last_updated = datetime.now(EASTERN).strftime("%Y-%m-%d %I:%M:%S %p")

        # --- Banners ---
        banner_rows = fetch_sheet_data(service, BANNERS_RANGE)
        if not banner_rows:
            logging.error("No banner data returned from Google Sheets.")
            return (json.dumps({"status": "error", "message": "No banner data from Sheets"}), 500, {"Content-Type": "application/json"})

        if not validate_headers(banner_rows, BANNER_HEADERS, "Banners"):
            return (json.dumps({"status": "error", "message": "Banner column structure changed — aborting"}), 500, {"Content-Type": "application/json"})
        banner_rows = banner_rows[1:]

        # --- Welcome Messages ---
        welcome_rows = fetch_sheet_data(service, WELCOME_RANGE)
        if not welcome_rows:
            logging.error("No welcome message data returned from Google Sheets.")
            return (json.dumps({"status": "error", "message": "No welcome data from Sheets"}), 500, {"Content-Type": "application/json"})

        if not validate_headers(welcome_rows, WELCOME_HEADERS, "Welcome"):
            return (json.dumps({"status": "error", "message": "Welcome column structure changed — aborting"}), 500, {"Content-Type": "application/json"})
        welcome_rows = welcome_rows[1:]

        # --- Geofence Locations ---
        geo_rows = fetch_sheet_data(service, GEO_RANGE)
        if not geo_rows:
            logging.error("No geofence data returned from Google Sheets.")
            return (json.dumps({"status": "error", "message": "No geofence data from Sheets"}), 500, {"Content-Type": "application/json"})

        if not validate_headers(geo_rows, GEO_HEADERS, "Geofences"):
            return (json.dumps({"status": "error", "message": "Geofence column structure changed — aborting"}), 500, {"Content-Type": "application/json"})
        geo_rows = geo_rows[1:]

        # Find single active entries (bottommost match wins)
        banner = find_active_banner(banner_rows, today)
        welcome = find_active_welcome(welcome_rows, today)

        # Write config
        config_ref = firebase_db.reference("config")
        config_ref.set({
            "lastUpdated": last_updated,
            "banner": banner,
            "heroHeader": welcome,
        })

        # Write geofences
        geofences = get_approved_geofences(geo_rows)

        geo_ref = firebase_db.reference("geo")
        geo_ref.set({
            "lastUpdated": last_updated,
            "locations": geofences,
        })

        logging.info(
            f"Synced — banner: {'active' if banner else 'none'}, "
            f"heroHeader: {'active' if welcome else 'none'}, "
            f"geofences: {len(geofences)} ({last_updated})"
        )
        return (json.dumps({
            "status": "ok",
            "banner": "active" if banner else "none",
            "heroHeader": "active" if welcome else "none",
            "geofences": len(geofences),
        }), 200, {"Content-Type": "application/json"})

    except google.auth.exceptions.DefaultCredentialsError as e:
        logging.error(f"Auth credentials not found: {e}")
        return (json.dumps({"status": "error", "message": f"Auth credentials not found: {e}"}), 500, {"Content-Type": "application/json"})

    except google.auth.exceptions.RefreshError as e:
        logging.error(f"Auth token refresh failed: {e}")
        return (json.dumps({"status": "error", "message": f"Auth token refresh failed: {e}"}), 500, {"Content-Type": "application/json"})

    except HttpError as e:
        status = e.resp.status
        if status == 403:
            msg = "Sheets API access denied — share the sheet with the service account"
        elif status == 404:
            msg = "Spreadsheet or range not found — check SPREADSHEET_ID and range"
        else:
            msg = f"Sheets API error ({status})"
        logging.error(f"{msg}: {e}")
        return (json.dumps({"status": "error", "message": msg}), 500, {"Content-Type": "application/json"})

    except Exception as e:
        logging.error(f"Unhandled error in sync_sheet: {e}")
        return (json.dumps({"status": "error", "message": str(e)}), 500, {"Content-Type": "application/json"})