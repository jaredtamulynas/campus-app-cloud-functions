import functions_framework
import json
import logging
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from html import unescape
from bs4 import BeautifulSoup
from google.cloud.logging import Client
from firebase_admin import initialize_app, firestore

# Initialize Firebase
initialize_app()
db = firestore.client()

# Set up Cloud Logging
Client().setup_logging()

LOCALIST_BASE_URL = "https://calendar.ncsu.edu/api/2/events"
EASTERN = ZoneInfo("America/New_York")


def fetch_localist_events():
    """Fetch events from the Localist calendar API (14 days, up to 2 pages)."""
    url = f"{LOCALIST_BASE_URL}?days=14&pp=100"
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    data = response.json()

    if not isinstance(data, dict):
        logging.error(f"Unexpected Localist response type: {type(data).__name__}")
        return []

    events = data.get("events", [])

    # Fetch second page if it exists
    total_pages = data.get("page", {}).get("total", 1)
    if total_pages >= 2:
        response2 = requests.get(f"{url}&page=2", timeout=15)
        response2.raise_for_status()
        events += response2.json().get("events", [])

    return events


def clean_html(html_str):
    """Strip HTML tags and unescape entities to plain text."""
    if not html_str:
        return ""
    return BeautifulSoup(unescape(html_str), "html.parser").get_text().strip()


def to_eastern(time_str):
    """Convert an ISO time string to Eastern time."""
    if not time_str:
        return None
    try:
        dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        return dt.astimezone(EASTERN).isoformat()
    except (ValueError, TypeError):
        return time_str


def format_event(item):
    """Transform a Localist event into the unified schema. Returns None for non-live events."""
    e = item.get("event", {})

    # Skip non-live events
    if e.get("status") != "live":
        return None

    geo = e.get("geo", {})

    instance = {}
    instances = e.get("event_instances", [])
    if instances:
        instance = instances[0].get("event_instance", {})

    event_id = e.get("id", "")
    instance_id = instance.get("id", "")

    categories = [t.get("name", "") for t in e.get("filters", {}).get("event_types", [])]
    topics = [t.get("name", "") for t in e.get("filters", {}).get("event_topic", [])]
    audience = [a.get("name", "") for a in e.get("filters", {}).get("event_target_audience", [])]

    departments = e.get("departments", [])
    department = departments[0].get("name") if departments else None

    try:
        lat = float(geo["latitude"]) if geo.get("latitude") else None
        lng = float(geo["longitude"]) if geo.get("longitude") else None
    except (ValueError, TypeError):
        lat, lng = None, None
    coordinate = {"lat": lat, "lng": lng} if lat and lng else None

    return {
        "id": f"localist_{event_id}_{instance_id}",
        "title": e.get("title", ""),
        "description": clean_html(e.get("description", "")) or e.get("description_text", ""),
        "start": to_eastern(instance.get("start")),
        "end": to_eastern(instance.get("end")),
        "allDay": instance.get("all_day", False),
        "location": {
            "name": e.get("location_name") or None,
            "room": e.get("room_number") or None,
            "address": e.get("address") or None,
            "coordinate": coordinate,
        },
        "url": e.get("localist_url") or e.get("url") or None,
        "imageUrl": e.get("photo_url") or None,
        "source": "localist",
        "experience": e.get("experience") or None,
        "isFree": e.get("free", True),
        "cost": e.get("ticket_cost") or None,
        "ticketUrl": e.get("ticket_url") or None,
        "categories": categories or None,
        "topics": topics or None,
        "targetAudience": audience or None,
        "department": department,
    }


@functions_framework.http
def get_calendar_events(request):
    try:
        raw_events = fetch_localist_events()
        events = [format_event(item) for item in raw_events]
        events = [e for e in events if e is not None]
        logging.info(f"Fetched {len(events)} live Localist events")

        if not events:
            logging.warning("No live Localist events found.")
            return (json.dumps({"status": "ok", "count": 0, "message": "No live events found"}), 200, {"Content-Type": "application/json"})

        today_str = datetime.now(EASTERN).date().isoformat()
        today_count = sum(
            1 for e in events
            if e.get("start") and e["start"].startswith(today_str)
        )

        events_dict = {e["id"]: e for e in events}

        timestamp = datetime.now(EASTERN).strftime("%Y-%m-%d %I:%M:%S %p")
        doc_ref = db.collection("universityCalendar").document("events")
        doc_ref.set({
            "items": events_dict,
            "todayCount": today_count,
            "lastUpdated": timestamp,
        })

        logging.info(f"Calendar events updated: {len(events)} total, {today_count} today")
        return (json.dumps({"status": "ok", "count": len(events), "todayCount": today_count}), 200, {"Content-Type": "application/json"})

    except Exception as e:
        logging.error(f"Unhandled error in get_calendar_events: {e}")
        return (json.dumps({"status": "error", "message": str(e)}), 500, {"Content-Type": "application/json"})