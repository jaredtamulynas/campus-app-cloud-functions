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
UTC = ZoneInfo("UTC")


def get_engage_headers():
    """Return auth headers for the Engage API."""
    return {"X-Engage-Api-Key": os.getenv("ENGAGE_API_KEY", "")}


# ── Events ───────────────────────────────────────────────────────────


def fetch_engage_events():
    """
    Fetch events from the Engage API (next 90 days).

    Constraints:
    - take=100
    - at most THREE pages (max 3 calls, 300 events max)
    """
    now_utc = datetime.now(UTC)
    starts_after = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    starts_before = (now_utc + timedelta(days=90)).replace(hour=0, minute=0, second=0, microsecond=0)

    all_items = []
    take = 100
    max_pages = 3

    for page in range(max_pages):
        skip = page * take
        params = {
            "startsAfter": starts_after.isoformat(),
            "startsBefore": starts_before.isoformat(),
            "take": take,
            "skip": skip,
        }

        resp = requests.get(
            ENGAGE_EVENTS_URL,
            headers=get_engage_headers(),
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        if isinstance(data, dict) and "error" in data:
            logging.error(f"Engage API error: {data['error']}")
            return []

        items = (data or {}).get("items", []) or []
        total = (data or {}).get("totalItems")

        all_items.extend(items)

        if not items:
            break

        if isinstance(total, int) and len(all_items) >= total:
            break

        if len(items) < take:
            break

    return all_items


# ── Organizations ────────────────────────────────────────────────────


def fetch_all_organizations():
    """
    Fetch all organizations from the Engage API (paginated).
    Active filtering is done client-side after fetching.

    - take=500, max 10 pages (5000 orgs max, early exit when done)
    """
    all_items = []
    take = 500
    max_pages = 10

    for page in range(max_pages):
        skip = page * take
        params = {"take": take, "skip": skip}

        try:
            resp = requests.get(
                ENGAGE_ORGS_URL,
                headers=get_engage_headers(),
                params=params,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logging.warning(f"Failed fetching orgs page {page}: {e}")
            break

        if isinstance(data, dict) and "error" in data:
            logging.error(f"Engage Orgs API error: {data['error']}")
            break

        items = (data or {}).get("items", []) or []
        total = (data or {}).get("totalItems")

        all_items.extend(items)

        if not items:
            break

        if isinstance(total, int) and len(all_items) >= total:
            break

        if len(items) < take:
            break

    return all_items


def fetch_organization_names_by_ids(org_ids):
    """Batch fetch organization names by IDs in a single call (for event hosting info)."""
    if not org_ids:
        return {}

    ids_params = "&".join(f"ids={oid}" for oid in org_ids)
    url = f"{ENGAGE_ORGS_URL}?{ids_params}&take=500"

    try:
        resp = requests.get(url, headers=get_engage_headers(), timeout=15)
        resp.raise_for_status()
        data = resp.json()

        if isinstance(data, dict) and "error" in data:
            logging.error(f"Engage Orgs API error: {data['error']}")
            return {}

        return {item["id"]: item.get("name", "") for item in (data or {}).get("items", []) or []}
    except Exception as e:
        logging.warning(f"Failed to fetch organization names: {e}")
        return {}


ENGAGE_IMAGE_BASE = "https://se-images.campuslabs.com/clink/images/"


def build_profile_image_url(raw):
    """Build a full image URL from the Engage profilePicture filename."""
    if not raw:
        return None
    return f"{ENGAGE_IMAGE_BASE}{raw}"


def format_organization(item):
    """Transform a raw Engage organization into a slim schema for Firestore."""
    org_type = item.get("organizationType") or {}

    return {
        "id": item.get("id"),
        "name": item.get("name", ""),
        "status": item.get("status") or None,
        "summary": clean_html(item.get("summary", "")),
        "description": clean_html(item.get("description", "")),
        "imageUrl": build_profile_image_url(item.get("profilePicture")),
        "organizationType": org_type.get("name") if isinstance(org_type, dict) else None,
    }


# ── Shared helpers ───────────────────────────────────────────────────


def clean_html(html_str):
    """Strip HTML tags and unescape entities to plain text."""
    if not html_str:
        return ""
    return BeautifulSoup(unescape(html_str), "html.parser").get_text().strip()


def to_eastern(time_str):
    """Convert an ISO time string to Eastern time (ISO string)."""
    if not time_str:
        return None
    try:
        dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        return dt.astimezone(EASTERN).isoformat()
    except (ValueError, TypeError):
        return time_str


def _safe_float(v):
    try:
        if v is None:
            return None
        return float(v)
    except (ValueError, TypeError):
        return None


def build_address_string(address):
    """Best-effort single-line address from Engage address fields."""
    if not address:
        return None

    if address.get("address"):
        return address.get("address")

    parts = []
    for key in ("line1", "line2"):
        if address.get(key):
            parts.append(address.get(key))

    city = address.get("city")
    state = address.get("state")
    zip_code = address.get("zip")
    city_state_zip = " ".join([p for p in [city, state, zip_code] if p])
    if city_state_zip:
        parts.append(city_state_zip)

    return ", ".join(parts) if parts else None


def is_public_event(item):
    """Only save events whose visibility is exactly Public (case-insensitive)."""
    return (item.get("visibility") or "").strip().lower() == "public"


def _unique_preserve_order(values):
    """Deduplicate while preserving order."""
    seen = set()
    out = []
    for v in values:
        if v is None:
            continue
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


# ── Display name maps for known ugly values ──────────────────────────

THEME_DISPLAY_NAMES = {
    "ThoughtfulLearning": "Thoughtful Learning",
    "CommunityService": "Community Service",
    "GroupBusiness": "Group Business",
    "SocialEvent": "Social Event",
}

BENEFIT_DISPLAY_NAMES = {
    "Merchandise": "Free Stuff",
}


def clean_theme(raw):
    """Map raw theme string to a display-friendly name."""
    if not raw:
        return None
    return THEME_DISPLAY_NAMES.get(raw, raw)


def clean_benefit(raw):
    """Map raw benefit string to a display-friendly name."""
    if not raw:
        return None
    return BENEFIT_DISPLAY_NAMES.get(raw, raw)


# ── Event formatting ─────────────────────────────────────────────────


def format_event(item, org_map):
    """
    Transform an Engage event into the unified schema.

    Supports multiple hosting orgs:
    - Uses organizationIds (if present) as the list of hosts
    - Falls back to submittedByOrganizationId if organizationIds is missing/empty
    """
    address = item.get("address", {}) or {}

    submitted_org_id = item.get("submittedByOrganizationId")
    submitted_org_name = org_map.get(submitted_org_id) if submitted_org_id else None

    org_ids = item.get("organizationIds") or []
    hosting_ids = org_ids if org_ids else ([submitted_org_id] if submitted_org_id else [])
    hosting_ids = _unique_preserve_order([oid for oid in hosting_ids if oid])

    hosting_organizations = (
        [{"id": oid, "name": org_map.get(oid) or None} for oid in hosting_ids] if hosting_ids else None
    )

    raw_categories = item.get("categories") or []
    categories = []
    for c in raw_categories:
        if not isinstance(c, dict):
            continue
        categories.append({
            "id": c.get("id"),
            "name": c.get("name"),
            "isVisibleOnPublicSite": c.get("isVisibleOnPublicSite"),
        })
    categories = categories or None

    lat = _safe_float(address.get("latitude"))
    lng = _safe_float(address.get("longitude"))
    coordinate = {"lat": lat, "lng": lng} if (lat is not None and lng is not None) else None

    state = item.get("state") or {}
    approval_status = state.get("status") if isinstance(state, dict) else None

    return {
        "id": item.get("id"),
        "title": item.get("name", ""),
        "url": f"https://getinvolved.ncsu.edu/event/{item.get('id', '')}",
        "description": clean_html(item.get("description", "")),
        "start": to_eastern(item.get("startsOn")),
        "end": to_eastern(item.get("endsOn")),

        "visibility": item.get("visibility") or None,

        "theme": clean_theme(item.get("theme")),
        "categories": categories,

        "location": {
            "name": address.get("name") or None,
            "address": build_address_string(address),
            "coordinate": coordinate,
            "onlineLocation": address.get("onlineLocation") or None,
            "instructions": address.get("instructions") or None,
        },

        "imageUrl": item.get("imageUrl") or None,
        "source": "engage",

        "benefits": [clean_benefit(b) for b in (item.get("benefits") or [])] or None,
        "type": item.get("type") or None,
        "approvalStatus": approval_status,

        "submittedByOrganization": (
            {"id": submitted_org_id, "name": submitted_org_name} if submitted_org_id else None
        ),

        "hostingOrganizations": hosting_organizations,
        "hostingOrganizationCount": len(hosting_ids),
    }


# ── Entry point ──────────────────────────────────────────────────────


@functions_framework.http
def get_organization_events(request):
    try:
        last_updated = datetime.now(EASTERN).strftime("%Y-%m-%d %I:%M:%S %p")

        # ── 1. Fetch & save organizations ────────────────────────────
        raw_orgs = fetch_all_organizations()
        logging.info(f"Fetched {len(raw_orgs)} total organizations from Engage")

        active_orgs = [o for o in raw_orgs if (o.get("status") or "").strip().lower() == "active"]
        logging.info(f"Filtered to {len(active_orgs)} active organizations")

        EXCLUDED_ORG_TYPES = {"test organization type", "branch"}
        active_orgs = [
            o for o in active_orgs
            if ((o.get("organizationType") or {}).get("name") or "").strip().lower() not in EXCLUDED_ORG_TYPES
        ]
        logging.info(f"After excluding org types: {len(active_orgs)} organizations")

        formatted_orgs = [format_organization(o) for o in active_orgs]
        orgs_dict = {str(o["id"]): o for o in formatted_orgs if o.get("id")}

        db.collection("getInvolved").document("organizations").set({
            "items": orgs_dict,
            "totalCount": len(orgs_dict),
            "lastUpdated": last_updated,
        })
        logging.info(f"Saved {len(orgs_dict)} organizations to getInvolved/organizations")

        # ── 2. Fetch & save events ───────────────────────────────────
        raw_events = fetch_engage_events()
        logging.info(f"Fetched {len(raw_events)} Engage events (raw)")

        public_raw_events = [e for e in raw_events if is_public_event(e)]
        logging.info(f"Filtered to {len(public_raw_events)} public Engage events")

        if not public_raw_events:
            db.collection("getInvolved").document("events").set({
                "items": {},
                "todayCount": 0,
                "lastUpdated": last_updated,
            })
            logging.warning("No public Engage events found; wrote empty items to Firestore.")
            return (json.dumps({"status": "ok", "organizations": len(orgs_dict), "events": 0}), 200, {"Content-Type": "application/json"})

        # Collect org IDs referenced by events for hosting name resolution
        event_org_ids = set()
        for item in public_raw_events:
            oid = item.get("submittedByOrganizationId")
            if oid:
                event_org_ids.add(oid)
            for other in (item.get("organizationIds") or []):
                if other:
                    event_org_ids.add(other)

        # Build org name map — prefer the full org list we already fetched,
        # fall back to a targeted fetch for any IDs we missed
        org_name_map = {o["id"]: o.get("name", "") for o in formatted_orgs if o.get("id")}
        missing_ids = event_org_ids - set(org_name_map.keys())
        if missing_ids:
            extra = fetch_organization_names_by_ids(missing_ids)
            org_name_map.update(extra)

        logging.info(f"Resolved {len(org_name_map)} organization names for events")

        events = [format_event(item, org_name_map) for item in public_raw_events]

        today_str = datetime.now(EASTERN).date().isoformat()
        today_count = sum(
            1 for e in events
            if e.get("start") and str(e["start"]).startswith(today_str)
        )

        events_dict = {str(e["id"]): e for e in events}

        db.collection("getInvolved").document("events").set({
            "items": events_dict,
            "todayCount": today_count,
            "lastUpdated": last_updated,
        })

        logging.info(f"Events updated: {len(events)} public total, {today_count} today")
        return (json.dumps({
            "status": "ok",
            "organizations": len(orgs_dict),
            "events": len(events),
            "todayCount": today_count,
        }), 200, {"Content-Type": "application/json"})

    except Exception as e:
        logging.exception(f"Unhandled error in get_organization_events: {e}")
        return (json.dumps({"status": "error", "message": str(e)}), 500, {"Content-Type": "application/json"})