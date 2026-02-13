import functions_framework
import json
import logging
import requests
import os
from datetime import datetime, timedelta
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

ENGAGE_EVENTS_URL = "https://engage-api.campuslabs.com/api/v3.0/events/event"
ENGAGE_ORGS_URL = "https://engage-api.campuslabs.com/api/v3.0/organizations/organization"
EASTERN = ZoneInfo("America/New_York")


def get_engage_headers():
    """Return auth headers for the Engage API."""
    return {"X-Engage-Api-Key": os.getenv("ENGAGE_API_KEY", "")}


def fetch_engage_events():
    """Fetch events from the Engage API (90 days)."""
    now = datetime.now(EASTERN)
    starts_after = now.strftime("%Y-%m-%dT00:00:00")
    starts_before = (now + timedelta(days=90)).strftime("%Y-%m-%dT00:00:00")

    url = f"{ENGAGE_EVENTS_URL}?startsAfter={starts_after}&startsBefore={starts_before}&take=100"
    response = requests.get(url, headers=get_engage_headers(), timeout=15)
    response.raise_for_status()
    data = response.json()

    # Check for API error response
    if isinstance(data, dict) and "error" in data:
        logging.error(f"Engage API error: {data['error']}")
        return []

    return data.get("items", [])


def fetch_organization_names(org_ids):
    """Batch fetch organization names by IDs in a single call."""
    if not org_ids:
        return {}

    ids_params = "&".join(f"ids={oid}" for oid in org_ids)
    url = f"{ENGAGE_ORGS_URL}?{ids_params}&take=500"

    try:
        response = requests.get(url, headers=get_engage_headers(), timeout=15)
        response.raise_for_status()
        data = response.json()

        if isinstance(data, dict) and "error" in data:
            logging.error(f"Engage Orgs API error: {data['error']}")
            return {}

        return {item["id"]: item.get("name", "") for item in data.get("items", [])}
    except Exception as e:
        logging.warning(f"Failed to fetch organization names: {e}")
        return {}


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


def format_event(item, org_map):
    """Transform an Engage event into the unified schema."""
    address = item.get("address", {}) or {}

    org_id = item.get("submittedByOrganizationId")
    organization = org_map.get(org_id) if org_id else None

    # Merge theme and categories into one list
    categories = []
    if item.get("theme"):
        categories.append(item["theme"])
    for cat in item.get("categories", []):
        name = cat.get("name", "")
        if name and name not in categories:
            categories.append(name)

    # Build coordinate if available
    lat = float(address["latitude"]) if address.get("latitude") else None
    lng = float(address["longitude"]) if address.get("longitude") else None
    coordinate = {"lat": lat, "lng": lng} if lat and lng else None

    return {
        "id": f"engage_{item.get('id', '')}",
        "title": item.get("name", ""),
        "description": clean_html(item.get("description", "")),
        "start": to_eastern(item.get("startsOn")),
        "end": to_eastern(item.get("endsOn")),
        "allDay": False,
        "location": {
            "name": address.get("name") or None,
            "address": address.get("address") or address.get("line1") or None,
            "coordinate": coordinate,
        },
        "url": None,
        "imageUrl": item.get("imageUrl") or None,
        "source": "engage",
        "categories": categories or None,
        "benefits": item.get("benefits") or None,
        "organization": organization,
    }


@functions_framework.cloud_event
def get_organization_events(cloud_event):
    try:
        # Fetch events
        raw_events = fetch_engage_events()
        logging.info(f"Fetched {len(raw_events)} Engage events")

        if not raw_events:
            logging.warning("No Engage events found.")
            return

        # Collect unique org IDs and batch fetch names
        org_ids = set()
        for item in raw_events:
            org_id = item.get("submittedByOrganizationId")
            if org_id:
                org_ids.add(org_id)

        org_map = fetch_organization_names(org_ids)
        logging.info(f"Resolved {len(org_map)} organization names")

        # Format events
        events = [format_event(item, org_map) for item in raw_events]

        # Count today's events
        today_str = datetime.now(EASTERN).date().isoformat()
        today_count = sum(
            1 for e in events
            if e.get("start") and e["start"].startswith(today_str)
        )

        # Build events dict keyed by ID
        events_dict = {e["id"]: e for e in events}

        # Write to Firestore
        timestamp = datetime.now(EASTERN).strftime("%Y-%m-%d %I:%M:%S %p")
        doc_ref = db.collection("events").document("organizationEvents")
        doc_ref.set({
            "items": events_dict,
            "todayCount": today_count,
            "lastUpdated": timestamp,
        })

        logging.info(f"Organization events updated: {len(events)} total, {today_count} today")

    except Exception as e:
        logging.error(f"Unhandled error in get_organization_events: {e}")