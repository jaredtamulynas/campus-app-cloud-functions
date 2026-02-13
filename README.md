# campus-cloud-functions

Cloud Functions that power [campus-app](https://github.com/YOUR_USERNAME/campus-app) — a configurable, open-source campus app for universities. These functions fetch data from campus APIs, normalize it into a consistent schema, and write to Firebase for real-time mobile consumption.

Built with Python on Google Cloud Run functions, triggered by Cloud Scheduler via Pub/Sub.

Starting with NC State, designed to be forked and configured for any campus.

---

## Architecture

```
Cloud Scheduler → Pub/Sub → Cloud Run functions → Firebase
                                  │
                     ┌────────────┼────────────────┐
                     │            │                 │
               Realtime DB    Firestore         FCM (Push)
               ├─ weather     ├─ events/        └─ rave-alert
               ├─ liveParking │  calendarEvents
               └─ liveCampus  │  organizationEvents
                  Busyness    └─
```

## Functions

| Function | Firebase Target | Provider | Schedule | Description |
|----------|----------------|----------|----------|-------------|
| `get-weather` | Realtime DB · `weather` | WeatherStem | Every 5 min | Temperature, feels-like, wind, UV, rain, sunrise/sunset, cloud camera |
| `get-live-parking` | Realtime DB · `liveParking` | OpenSpace | Every 5 min | Lot availability, occupancy, coordinates |
| `get-live-campus-busyness` | Realtime DB · `liveCampusBusyness` | Waitz | Every 5 min | Facility occupancy with sub-locations and status labels |
| `get-calendar-events` | Firestore · `events/calendarEvents` | Localist | Daily 4:00 AM | University academic calendar (7 days) |
| `get-organization-events` | Firestore · `events/organizationEvents` | CampusLabs Engage | Daily 4:30 AM | Student org events with org names (90 days) |
| `get-rave-alert` | Realtime DB · `raveAlert` + FCM | Rave Mobile Safety | Every 2 min | Emergency alerts via RSS with push notifications |

## Prerequisites

- Google Cloud project with billing enabled
- Firebase project (Realtime Database + Firestore + Cloud Messaging)
- Python 3.14+
- API keys for your campus data providers

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/campus-cloud-functions.git
cd campus-cloud-functions
```

### 2. Configure Google Cloud

Using the Google Cloud Console:

1. **Secret Manager** — store your API keys (`WEATHERSTEM_API_KEY`, `OPENSPACE_API_KEY`, `ENGAGE_API_KEY`)
2. **Pub/Sub** — create a topic for each function (e.g., `get-weather`, `get-live-parking`, etc.)
3. **Cloud Run functions** — deploy each function folder with its Pub/Sub trigger, runtime `python314`, and attach any required secrets as environment variables
4. **Cloud Scheduler** — create a job for each function pointing to its Pub/Sub topic

### Schedules

| Function | Cron | Timezone |
|----------|------|----------|
| `get-weather` | `*/5 * * * *` | — |
| `get-live-parking` | `*/5 * * * *` | — |
| `get-live-campus-busyness` | `*/5 * * * *` | — |
| `get-calendar-events` | `0 4 * * *` | America/New_York |
| `get-organization-events` | `30 4 * * *` | America/New_York |
| `get-rave-alert` | `*/2 * * * *` | — |

### Entry Points

Each function's entry point matches its folder name with underscores:

| Folder | Entry Point |
|--------|-------------|
| `get-weather` | `get_weather` |
| `get-live-parking` | `get_live_parking` |
| `get-live-campus-busyness` | `get_live_campus_busyness` |
| `get-calendar-events` | `get_calendar_events` |
| `get-organization-events` | `get_organization_events` |
| `get-rave-alert` | `get_rave` |

## Firebase Schema

All data follows consistent conventions:

- **Field names:** camelCase
- **Timestamps:** `yyyy-MM-dd hh:mm:ss a` Eastern time, DST-aware
- **Location objects:** `{ name, address, coordinate: { lat, lng } }`
- **Occupancy:** Raw integers from the source API

### Realtime Database

```
├── weather
│   ├── temperature: 72
│   ├── feelsLike: 68
│   ├── humidity: 45
│   ├── wind: { speed, gust, direction, degrees }
│   ├── uvIndex: 3
│   ├── rain: { rate, total }
│   ├── sunrise: 1707820800
│   ├── sunset: 1707858600
│   ├── imageUrl: "https://..."
│   └── lastUpdated: "2026-02-13 02:45:08 PM"
│
├── liveParking
│   ├── lastUpdated: "2026-02-13 02:45:08 PM"
│   └── lots
│       └── danAllenDeck
│           ├── id: "danAllenDeck"
│           ├── name: "Dan Allen Deck"
│           ├── location: { address, coordinate: { lat, lng } }
│           ├── totalSpaces: 1200
│           ├── availableSpaces: 340
│           ├── occupancy: 860
│           └── isHidden: false
│
├── liveCampusBusyness
│   ├── lastUpdated: "2026-02-13 02:45:08 PM"
│   └── locations
│       └── "126"
│           ├── id: 126
│           ├── name: "Wellness & Rec Center"
│           ├── occupancy: 78
│           ├── capacity: 518
│           ├── isOpen: true
│           ├── status: "high"
│           ├── bestSpot: "Level 4 - Functional Training"
│           └── subLocations: [...]
│
└── raveAlert
    ├── title: "WolfAlert: ..."
    ├── description: "..."
    ├── link: "https://..."
    └── pubDate: "2026-02-13T17:03:58+00:00"
```

### Firestore

```
events/
├── calendarEvents
│   ├── lastUpdated: "2026-02-13 04:00:12 AM"
│   ├── todayCount: 8
│   └── items
│       └── localist_51450366704614
│           ├── id: "localist_51450366704614"
│           ├── title: "Drop/Revision Deadline"
│           ├── description: "..."
│           ├── start: "2026-02-12T00:00:00-05:00"
│           ├── end: null
│           ├── allDay: true
│           ├── location: { name, address, coordinate }
│           ├── url: "https://calendar.ncsu.edu/..."
│           ├── imageUrl: "https://..."
│           ├── source: "localist"
│           ├── categories: ["Academic Calendar"]
│           └── department: "Student Services"
│
└── organizationEvents
    ├── lastUpdated: "2026-02-13 04:30:08 AM"
    ├── todayCount: 3
    └── items
        └── engage_12030283
            ├── id: "engage_12030283"
            ├── title: "Bhakti Yoga Club Weekly Meetings"
            ├── description: "..."
            ├── start: "2026-02-18T18:30:00-05:00"
            ├── end: "2026-02-18T20:00:00-05:00"
            ├── allDay: false
            ├── location: { name, address, coordinate }
            ├── url: null
            ├── imageUrl: "https://se-images.campuslabs.com/..."
            ├── source: "engage"
            ├── categories: ["Spirituality"]
            ├── benefits: ["FreeFood", "Merchandise"]
            └── organization: "Bhakti Yoga Club"
```

## Adapting for Your Campus

Each function fetches from a specific provider. To adapt for a different university:

1. **Update API URLs and keys** — swap the base URLs and credentials for your campus providers
2. **Adjust the formatter functions** — `format_event()`, `build_lot_data()`, etc. map provider-specific fields to the shared schema
3. **Keep the schema** — the Firebase structure and field names stay the same regardless of provider, so your app code works without changes

| Data Type | Common Providers |
|-----------|-----------------|
| Weather | WeatherStem, OpenWeatherMap, campus weather stations |
| Parking | OpenSpace, ParkMobile, Passport, campus-specific APIs |
| Busyness | Waitz, SafeSpace, campus occupancy sensors |
| Calendar | Localist, 25Live, university calendar APIs |
| Org Events | CampusLabs Engage, OrgSync, Presence |
| Alerts | Rave Mobile Safety, Omnilert, Alertus |

## Error Handling

All functions follow the same pattern to prevent Pub/Sub retry loops:

```python
@functions_framework.cloud_event
def function_name(cloud_event):
    try:
        # All logic here
        pass
    except Exception as e:
        logging.error(f"Unhandled error: {e}")
        # Clean return = 200 = message acknowledged = no retry
```

Without this, a 500 response triggers infinite Pub/Sub retries (push subscriptions have no max-retry setting).

## Project Structure

```
campus-cloud-functions/
├── get-weather/
│   ├── main.py
│   └── requirements.txt
├── get-live-parking/
│   ├── main.py
│   └── requirements.txt
├── get-live-campus-busyness/
│   ├── main.py
│   └── requirements.txt
├── get-calendar-events/
│   ├── main.py
│   └── requirements.txt
├── get-organization-events/
│   ├── main.py
│   └── requirements.txt
├── get-rave-alert/
│   ├── main.py
│   └── requirements.txt
└── README.md
```

## Related

- [campus-app](https://github.com/YOUR_USERNAME/campus-app) — SwiftUI + Firebase mobile app that consumes this data

## License

MIT
