import functions_framework
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from collections import defaultdict
from google.cloud.logging import Client
from firebase_admin import initialize_app, firestore
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import google.auth.exceptions

# Initialize Firebase
initialize_app()
db = firestore.client()

# Set up Cloud Logging
Client().setup_logging()

EASTERN = ZoneInfo("America/New_York")

# Google Sheets config
SPREADSHEET_ID = "1lEf0viWxAuUZlfakzZMY62cAN0hIzFjqn3xVwTn0I5Y"
LOCATIONS_RANGE = "Locations!A1:J"
WEEKLY_HOURS_RANGE = "Weekly Hours!A3:BC"
SPECIAL_HOURS_RANGE = "Special Hours!A7:N"
CONFIG_FOOD_COURTS_RANGE = "Config!A1:B"
CONFIG_LOCATION_TYPES_RANGE = "Config!D1:E"

# Expected column headers (lowercase for comparison)
LOCATIONS_HEADERS = ["id", "location key", "location name", "location type", "food court", "building id", "menu data", "hide", "menu key", "menu key 2"]
SPECIAL_HOURS_HEADERS = ["id", "location key", "location name", "priority", "label", "start date", "end date", "status", "period 1 open", "period 1 close", "period 2 open", "period 2 close", "period 3 open", "period 3 close"]

# Weekly hours sub-header pattern: ID, Location Key, Location Name, then 7x (Status, P1 Open, P1 Close, P2 Open, P2 Close, P3 Open, P3 Close)
WEEKLY_HOURS_FIRST_HEADERS = ["id", "location key", "location name"]
WEEKLY_HOURS_DAY_BLOCK = ["status", "period 1 open", "period 1 close", "period 2 open", "period 2 close", "period 3 open", "period 3 close"]

DAY_NAMES = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

# Each day block: Status + Period1Open + Period1Close + Period2Open + Period2Close + Period3Open + Period3Close = 7 columns
DAY_BLOCK_SIZE = 7


def normalize_header(cell):
    """Normalize a header cell: lowercase, replace newlines with spaces, strip."""
    return cell.strip().lower().replace("\n", " ")


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

    actual = [normalize_header(cell) for cell in rows[0]]
    actual_subset = actual[:len(expected_headers)]

    if actual_subset != expected_headers:
        logging.error(
            f"{section_name}: Column mismatch — "
            f"expected {expected_headers}, got {actual_subset}"
        )
        return False

    return True


def validate_weekly_hours_headers(rows):
    """Validate weekly hours header structure.
    Row 1 (from range): ID, Location Key, Location Name, Status, P1 Open, P1 Close, ... repeated 7x
    Validates first 3 columns then checks each day block has the correct sub-header pattern.
    """
    if not rows:
        logging.error("Weekly Hours: No rows returned")
        return False

    actual = [normalize_header(cell) for cell in rows[0]]

    # Check first 3 columns
    if len(actual) < 3:
        logging.error("Weekly Hours: Too few columns")
        return False

    if actual[:3] != WEEKLY_HOURS_FIRST_HEADERS:
        logging.error(f"Weekly Hours: First 3 columns mismatch — expected {WEEKLY_HOURS_FIRST_HEADERS}, got {actual[:3]}")
        return False

    # Check each day block has the correct pattern
    expected_total = 3 + (7 * DAY_BLOCK_SIZE)  # 52 columns
    if len(actual) < expected_total:
        logging.error(f"Weekly Hours: Expected at least {expected_total} columns, got {len(actual)}")
        return False

    for i in range(7):
        base = 3 + (i * DAY_BLOCK_SIZE)
        block = actual[base:base + DAY_BLOCK_SIZE]
        if block != WEEKLY_HOURS_DAY_BLOCK:
            logging.error(f"Weekly Hours: Day block {i} (col {base}) mismatch — expected {WEEKLY_HOURS_DAY_BLOCK}, got {block}")
            return False

    return True


def safe_get(row, idx, default=""):
    """Safely get a value from a row by index."""
    return row[idx].strip() if idx < len(row) and row[idx] else default


def parse_bool(value):
    """Parse a string boolean value from the sheet."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return False


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


def parse_menu_key2(value):
    """Parse menuKey2 — could be empty, a single value, or comma-separated list."""
    if not value or value.strip().lower() in ("", "null", "none"):
        return None
    parts = [v.strip() for v in value.split(",") if v.strip()]
    if len(parts) == 0:
        return None
    if len(parts) == 1:
        return parts[0]
    return parts


def parse_time(value):
    """Normalize a time string. Returns None if empty/invalid."""
    if not value or not value.strip():
        return None
    return value.strip()


def parse_periods(row, base_index):
    """Parse up to 3 open/close period pairs starting at base_index.
    Expects pairs at: base+0/base+1, base+2/base+3, base+4/base+5
    """
    periods = []
    for p in range(3):
        open_time = parse_time(safe_get(row, base_index + (p * 2)))
        close_time = parse_time(safe_get(row, base_index + 1 + (p * 2)))
        if open_time and close_time:
            periods.append({"open": open_time, "close": close_time})
    return periods


# ── Locations ────────────────────────────────────────────────────────


def parse_location(row):
    """Parse a single row from the Locations sheet.
    Columns: ID(0), Location Key(1), Location Name(2), Location Type(3),
             Food Court(4), Building ID(5), Menu Data(6), Hide(7), Menu Key(8), Menu Key 2(9)
    """
    location_id = safe_get(row, 0)
    location_key = safe_get(row, 1)

    if not location_id or not location_key:
        return None

    try:
        location_id = int(location_id)
    except ValueError:
        return None

    food_court = safe_get(row, 4)
    if food_court.lower() in ("", "false", "null", "none"):
        food_court = None

    entry = {
        "id": location_id,
        "locationKey": location_key,
        "name": safe_get(row, 2),
        "locationType": safe_get(row, 3),
        "buildingId": safe_get(row, 5),
        "hasMenu": parse_bool(safe_get(row, 6, "false")),
        "display": not parse_bool(safe_get(row, 7, "false")),
        "menuKey": safe_get(row, 8),
        "imageURL": f"https://storage.googleapis.com/storage-oncampus/dining/logos/{location_key}.png",
    }

    if food_court:
        entry["foodCourt"] = food_court

    menu_key2 = parse_menu_key2(safe_get(row, 9))
    if menu_key2:
        entry["menuKey2"] = menu_key2

    return entry


# ── Weekly Hours ─────────────────────────────────────────────────────


def parse_day_hours(row, day_index):
    """Parse a single day's hours from a weekly hours row.
    Each day block starts at column 3 + (day_index * 7) and has 7 columns:
    Status(0), P1 Open(1), P1 Close(2), P2 Open(3), P2 Close(4), P3 Open(5), P3 Close(6)
    """
    base = 3 + (day_index * DAY_BLOCK_SIZE)

    status_raw = safe_get(row, base).lower()
    status = "open" if status_raw == "open" else "closed"

    if status == "closed":
        return {"status": "closed", "periods": []}

    periods = parse_periods(row, base + 1)

    return {
        "status": "open" if periods else "closed",
        "periods": periods,
    }


def parse_weekly_hours_row(row):
    """Parse a single row from the Weekly Hours sheet.
    Returns (location_key, hours_dict) or (None, None) if invalid.
    """
    location_key = safe_get(row, 1)
    if not location_key:
        return None, None

    hours = {}
    for i, day in enumerate(DAY_NAMES):
        hours[day] = parse_day_hours(row, i)

    return location_key, hours


# ── Special Hours ────────────────────────────────────────────────────


def parse_special_hours_row(row):
    """Parse a single row from the Special Hours sheet.
    Columns: ID(0), Location Key(1), Location Name(2), Priority(3), Label(4),
             Start Date(5), End Date(6), Status(7),
             P1 Open(8), P1 Close(9), P2 Open(10), P2 Close(11), P3 Open(12), P3 Close(13)
    """
    location_key = safe_get(row, 1)
    if not location_key:
        return None

    label = safe_get(row, 4)
    start = parse_date(safe_get(row, 5))
    end = parse_date(safe_get(row, 6))

    if not label or not start or not end:
        return None

    try:
        priority = int(safe_get(row, 3, "0"))
    except ValueError:
        priority = 0

    status_raw = safe_get(row, 7).lower()
    status = "open" if status_raw == "open" else "closed"

    periods = []
    if status == "open":
        periods = parse_periods(row, 8)

    return {
        "locationKey": location_key,
        "priority": priority,
        "label": label,
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "status": status,
        "periods": periods,
    }


def build_special_hours_by_location(special_hours_list):
    """Group special hours entries by locationKey for O(1) lookup.

    "all" key holds entries that apply to every location.
    Each location key holds entries specific to that location.
    """
    by_location = defaultdict(list)
    for entry in special_hours_list:
        key = entry["locationKey"]
        by_location[key].append({
            "priority": entry["priority"],
            "label": entry["label"],
            "startDate": entry["startDate"],
            "endDate": entry["endDate"],
            "status": entry["status"],
            "periods": entry["periods"],
        })
    return dict(by_location)


# ── Today Hours Resolution ──────────────────────────────────────────


def resolve_today_hours(location_keys, weekly_hours, special_hours_by_location, today):
    """Resolve today's hours for every location.

    Priority logic:
    1. Gather entries from special_hours_by_location["all"] + special_hours_by_location[locationKey]
    2. Filter to entries where today falls in [startDate, endDate]
    3. Highest priority number wins; location-specific beats "all" at same priority
    4. If no special hours match, fall back to weekly hours for today's day name

    Returns: dict of locationKey -> {status, periods, label}
    """
    today_iso = today.isoformat()
    day_name = DAY_NAMES[today.weekday()]

    # Pre-filter "all" entries active today
    all_entries = [
        e for e in special_hours_by_location.get("all", [])
        if e["startDate"] <= today_iso <= e["endDate"]
    ]
    best_all = max(all_entries, key=lambda e: e["priority"]) if all_entries else None

    today_hours = {}
    for loc_key in location_keys:
        # Location-specific entries active today
        loc_entries = [
            e for e in special_hours_by_location.get(loc_key, [])
            if e["startDate"] <= today_iso <= e["endDate"]
        ]
        best_loc = max(loc_entries, key=lambda e: e["priority"]) if loc_entries else None

        # Pick winner: compare location-specific vs "all"
        chosen = None
        if best_loc and best_all:
            chosen = best_all if best_all["priority"] > best_loc["priority"] else best_loc
        elif best_loc:
            chosen = best_loc
        elif best_all:
            chosen = best_all

        if chosen:
            today_hours[loc_key] = {
                "status": chosen["status"],
                "periods": chosen["periods"],
                "label": chosen["label"],
            }
        else:
            weekly = weekly_hours.get(loc_key, {}).get(day_name)
            if weekly:
                today_hours[loc_key] = {
                    "status": weekly["status"],
                    "periods": weekly["periods"],
                    "label": None,
                }
            else:
                today_hours[loc_key] = {
                    "status": "closed",
                    "periods": [],
                    "label": None,
                }

    return today_hours


# ── Config ───────────────────────────────────────────────────────────


def to_camel_case(text):
    """Convert 'Food Courts' to 'foodCourts'."""
    words = text.strip().split()
    if not words:
        return ""
    return words[0].lower() + "".join(w.capitalize() for w in words[1:])


def parse_config_section(rows):
    """Parse a config section.
    Row 1: Section title (e.g. 'Food Courts')
    Row 2: Column headers (Key, Value) — skipped
    Row 3+: Data rows

    Returns: (camelCase key, display name, dict of {key: displayName})
    """
    if not rows or len(rows) < 3:
        return None, None, {}

    display_name = safe_get(rows[0], 0)
    key = to_camel_case(display_name)

    items = {}
    for row in rows[2:]:
        k = safe_get(row, 0)
        v = safe_get(row, 1)
        if k and v:
            items[k] = v

    return key, display_name, items


# ── Entry Point ──────────────────────────────────────────────────────


@functions_framework.http
def sync_sheet(request):
    """HTTP-triggered function that reads dining location, hours, and config data
    from Google Sheets and writes to Firestore."""
    try:
        service = get_sheets_service()
        now = datetime.now(EASTERN)
        today = now.date()
        last_updated = now.strftime("%Y-%m-%d %I:%M:%S %p")

        # ── 1. Locations ─────────────────────────────────────────────
        location_rows = fetch_sheet_data(service, LOCATIONS_RANGE)
        if not location_rows:
            logging.error("No location data returned from Google Sheets.")
            return (json.dumps({"status": "error", "message": "No location data from Sheets"}), 500, {"Content-Type": "application/json"})

        if not validate_headers(location_rows, LOCATIONS_HEADERS, "Locations"):
            return (json.dumps({"status": "error", "message": "Locations column structure changed — aborting"}), 500, {"Content-Type": "application/json"})
        location_rows = location_rows[1:]

        locations = []
        location_keys = []
        skipped = 0
        for row in location_rows:
            location = parse_location(row)
            if location:
                locations.append(location)
                location_keys.append(location["locationKey"])
            else:
                skipped += 1

        if not locations:
            logging.error("No valid locations parsed.")
            return (json.dumps({"status": "error", "message": "No valid locations parsed"}), 500, {"Content-Type": "application/json"})

        db.collection("dining").document("locations").set({
            "locations": locations,
            "lastUpdated": last_updated,
        })
        logging.info(f"Saved {len(locations)} locations (skipped {skipped})")

        # ── 2. Weekly Hours ──────────────────────────────────────────
        weekly_rows = fetch_sheet_data(service, WEEKLY_HOURS_RANGE)
        if not weekly_rows:
            logging.error("No weekly hours data returned from Google Sheets.")
            return (json.dumps({"status": "error", "message": "No weekly hours data from Sheets"}), 500, {"Content-Type": "application/json"})

        if not validate_weekly_hours_headers(weekly_rows):
            return (json.dumps({"status": "error", "message": "Weekly Hours column structure changed — aborting"}), 500, {"Content-Type": "application/json"})
        weekly_data_rows = weekly_rows[1:]

        weekly_hours = {}
        for row in weekly_data_rows:
            location_key, hours = parse_weekly_hours_row(row)
            if location_key and hours:
                weekly_hours[location_key] = hours

        db.collection("dining").document("weeklyHours").set({
            "hours": weekly_hours,
            "lastUpdated": last_updated,
        })
        logging.info(f"Saved weekly hours for {len(weekly_hours)} locations")

        # ── 3. Special Hours ─────────────────────────────────────────
        special_rows = fetch_sheet_data(service, SPECIAL_HOURS_RANGE)
        special_hours_list = []

        if special_rows:
            if not validate_headers(special_rows, SPECIAL_HOURS_HEADERS, "Special Hours"):
                return (json.dumps({"status": "error", "message": "Special Hours column structure changed — aborting"}), 500, {"Content-Type": "application/json"})
            special_data_rows = special_rows[1:]

            for row in special_data_rows:
                entry = parse_special_hours_row(row)
                if entry:
                    special_hours_list.append(entry)

        special_hours_by_location = build_special_hours_by_location(special_hours_list)

        db.collection("dining").document("specialHours").set({
            "byLocation": special_hours_by_location,
            "lastUpdated": last_updated,
        })
        logging.info(f"Saved {len(special_hours_list)} special hours entries across {len(special_hours_by_location)} keys")

        # ── 4. Today Hours (pre-resolved) ────────────────────────────
        today_hours = resolve_today_hours(
            location_keys, weekly_hours, special_hours_by_location, today
        )

        open_count = sum(1 for h in today_hours.values() if h["status"] == "open")

        db.collection("dining").document("todayHours").set({
            "hours": today_hours,
            "date": today.isoformat(),
            "openCount": open_count,
            "lastUpdated": last_updated,
        })
        logging.info(f"Saved today hours: {open_count} open, {len(today_hours) - open_count} closed")

        # ── 5. Config ────────────────────────────────────────────────
        fc_rows = fetch_sheet_data(service, CONFIG_FOOD_COURTS_RANGE)
        lt_rows = fetch_sheet_data(service, CONFIG_LOCATION_TYPES_RANGE)

        config_doc = {}

        fc_key, fc_display, fc_items = parse_config_section(fc_rows)
        if fc_key and fc_items:
            config_doc[fc_key] = {
                "displayName": fc_display,
                "items": fc_items,
            }

        lt_key, lt_display, lt_items = parse_config_section(lt_rows)
        if lt_key and lt_items:
            config_doc[lt_key] = {
                "displayName": lt_display,
                "items": lt_items,
            }

        if config_doc:
            db.collection("dining").document("types").set(config_doc)

        logging.info(
            f"Dining synced — locations: {len(locations)}, "
            f"weeklyHours: {len(weekly_hours)}, "
            f"specialHours: {len(special_hours_list)}, "
            f"todayOpen: {open_count}, "
            f"foodCourts: {len(fc_items)}, types: {len(lt_items)} "
            f"({last_updated})"
        )
        return (json.dumps({
            "status": "ok",
            "locations": len(locations),
            "weeklyHours": len(weekly_hours),
            "specialHours": len(special_hours_list),
            "todayOpen": open_count,
            "foodCourts": len(fc_items),
            "locationTypes": len(lt_items),
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