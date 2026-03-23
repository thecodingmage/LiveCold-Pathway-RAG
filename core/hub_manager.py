"""
LiveCold — Intelligent Cold Storage Hub Manager
Real-time hub availability with traffic-aware ETA, capacity matching, and temperature zone compliance.
"""

import math
import random
import time
import threading
from datetime import datetime


# ══════════════════════════════════════════════════════════════════
#  Temperature Zone System
# ══════════════════════════════════════════════════════════════════

TEMP_ZONES = {
    "ultra_cold":  {"range": (-80, -40), "label": "Ultra-Cold",  "color": "#9c27b0"},
    "deep_frozen": {"range": (-40, -25), "label": "Deep Frozen", "color": "#3f51b5"},
    "frozen":      {"range": (-25, -10), "label": "Frozen",      "color": "#2196f3"},
    "chilled":     {"range": (-2,   8),  "label": "Chilled",     "color": "#00bcd4"},
    "cool":        {"range": (8,   15),  "label": "Cool",        "color": "#4caf50"},
    "ambient":     {"range": (15,  25),  "label": "Ambient",     "color": "#ff9800"},
}

# Map product types → required temp zones
PRODUCT_TEMP_ZONES = {
    "vaccines":    ["ultra_cold", "deep_frozen", "chilled"],  # varies by vaccine
    "frozen_meat": ["frozen", "deep_frozen"],
    "dairy":       ["chilled"],
    "seafood":     ["chilled", "frozen"],
    "pharmaceuticals": ["chilled", "cool"],
    "ice_cream":   ["frozen", "deep_frozen"],
    "fruits":      ["cool", "ambient"],
    "flowers":     ["cool"],
}


def get_required_zones(product_type):
    """Get required temperature zones for a product."""
    key = product_type.lower().replace(" ", "_")
    return PRODUCT_TEMP_ZONES.get(key, ["chilled"])  # default to chilled


def zone_matches(hub_zones, product_type):
    """Check if hub supports at least one required temp zone for this product."""
    required = get_required_zones(product_type)
    return any(z in hub_zones for z in required)


# ══════════════════════════════════════════════════════════════════
#  Traffic Model (time-of-day + city/highway factors)
# ══════════════════════════════════════════════════════════════════

# Realistic traffic multipliers by hour (IST)
TRAFFIC_PROFILE = {
    # hour: multiplier (1.0 = free flow at 55 km/h)
    0: 0.85, 1: 0.80, 2: 0.80, 3: 0.82, 4: 0.85, 5: 0.90,
    6: 1.10, 7: 1.30, 8: 1.60, 9: 1.80, 10: 1.40, 11: 1.20,
    12: 1.15, 13: 1.10, 14: 1.15, 15: 1.20, 16: 1.35, 17: 1.70,
    18: 1.90, 19: 1.60, 20: 1.30, 21: 1.10, 22: 0.95, 23: 0.90,
}

# City congestion zones (lat, lon, radius_km, name, extra_delay_factor)
CONGESTION_ZONES = [
    (28.63, 77.22, 25, "Delhi NCR",     1.8),
    (19.08, 72.88, 20, "Mumbai Metro",  1.7),
    (12.97, 77.59, 15, "Bangalore CBD", 1.5),
    (13.08, 80.27, 12, "Chennai City",  1.4),
    (22.57, 88.36, 15, "Kolkata",       1.5),
    (17.39, 78.49, 12, "Hyderabad",     1.3),
    (23.02, 72.57, 10, "Ahmedabad",     1.3),
    (18.52, 73.81, 10, "Pune",          1.2),
]

# Highway speed tiers
HIGHWAY_BASE_SPEED = 55    # km/h on national highway
CITY_BASE_SPEED = 25       # km/h in city
SEMI_URBAN_SPEED = 40      # km/h peri-urban


def _get_congestion_factor(lat, lon):
    """Check if location is in a congestion zone."""
    for clat, clon, radius, name, factor in CONGESTION_ZONES:
        dist = _haversine_km(lat, lon, clat, clon)
        if dist < radius:
            return factor, name
    return 1.0, None


def compute_eta_minutes(curr_lat, curr_lon, dest_lat, dest_lon, distance_km):
    """
    Dynamic traffic-aware ETA from truck's CURRENT GPS position to hub.

    Two-phase calculation:
      Phase 1: Estimate highway transit time using current-hour traffic
      Phase 2: Re-calculate destination entry time using ARRIVAL-HOUR traffic
               (e.g. leaving at 4 PM, arriving at 9 PM → use 9 PM congestion)
    """
    now = datetime.now()
    current_hour = now.hour

    # ── Phase 1: Current-location traffic ──
    # Traffic factor for the origin (truck's current position)
    curr_time_factor = TRAFFIC_PROFILE.get(current_hour, 1.0)
    curr_cong, curr_zone = _get_congestion_factor(curr_lat, curr_lon)

    # Segment the journey: city exit (10%) → highway (80%) → city entry (10%)
    city_exit_km = min(distance_km * 0.10, 15)
    city_entry_km = min(distance_km * 0.10, 15)
    highway_km = max(distance_km - city_exit_km - city_entry_km, 0)

    # Exit speed: current location congestion × current time-of-day
    exit_speed = max(CITY_BASE_SPEED / curr_cong / curr_time_factor, 5)
    highway_speed = max(HIGHWAY_BASE_SPEED / curr_time_factor, 15)

    exit_time_min = (city_exit_km / exit_speed) * 60
    highway_time_min = (highway_km / highway_speed) * 60

    # ── Phase 2: Estimate arrival hour, then get destination traffic ──
    transit_hours = (exit_time_min + highway_time_min) / 60
    arrival_hour = int((current_hour + transit_hours) % 24)

    # Use ARRIVAL-HOUR traffic for destination city, not current hour
    arrival_time_factor = TRAFFIC_PROFILE.get(arrival_hour, 1.0)
    dest_cong, dest_zone = _get_congestion_factor(dest_lat, dest_lon)

    entry_speed = max(CITY_BASE_SPEED / dest_cong / arrival_time_factor, 5)
    entry_time_min = (city_entry_km / entry_speed) * 60

    total_minutes = exit_time_min + highway_time_min + entry_time_min

    # Add jitter ±5% for realism
    total_minutes *= random.uniform(0.95, 1.05)

    # Determine overall traffic severity
    peak_factor = max(curr_time_factor, arrival_time_factor)

    return {
        "eta_minutes": round(total_minutes),
        "avg_speed_kmph": round(distance_km / (total_minutes / 60), 1) if total_minutes > 0 else 0,
        "traffic_level": "heavy" if peak_factor > 1.5 else "moderate" if peak_factor > 1.1 else "light",
        "departure_traffic": round(curr_time_factor, 2),
        "arrival_traffic": round(arrival_time_factor, 2),
        "arrival_hour": arrival_hour,
        "curr_zone": curr_zone,
        "dest_zone": dest_zone,
    }


# ══════════════════════════════════════════════════════════════════
#  Cold Storage Hub Database
# ══════════════════════════════════════════════════════════════════

HUBS = [
    {"id": "HUB_01", "name": "Snowman Logistics - Chennai",   "lat": 13.0670, "lon": 80.2370, "capacity_tonnes": 500,  "occupied_pct": 65, "temp_zones": ["chilled", "frozen"],                "status": "available", "contact": "+91-9876543201", "has_repair": True},
    {"id": "HUB_02", "name": "Coldstar Warehouse - Bangalore", "lat": 12.9352, "lon": 77.6245, "capacity_tonnes": 800,  "occupied_pct": 72, "temp_zones": ["chilled", "frozen", "deep_frozen"], "status": "available", "contact": "+91-9876543202", "has_repair": True},
    {"id": "HUB_03", "name": "FrostLine Cold Store - Delhi",   "lat": 28.6280, "lon": 77.2200, "capacity_tonnes": 1200, "occupied_pct": 85, "temp_zones": ["chilled", "frozen", "deep_frozen", "ultra_cold"], "status": "available", "contact": "+91-9876543203", "has_repair": True},
    {"id": "HUB_04", "name": "Arctic Storage - Mumbai",        "lat": 19.0330, "lon": 72.8500, "capacity_tonnes": 1000, "occupied_pct": 58, "temp_zones": ["chilled", "frozen"],                "status": "available", "contact": "+91-9876543204", "has_repair": True},
    {"id": "HUB_05", "name": "IcePack Logistics - Hyderabad",  "lat": 17.4065, "lon": 78.4772, "capacity_tonnes": 600,  "occupied_pct": 45, "temp_zones": ["chilled", "frozen", "ultra_cold"],  "status": "available", "contact": "+91-9876543205", "has_repair": True},
    {"id": "HUB_06", "name": "ColdChain India - Kolkata",      "lat": 22.5480, "lon": 88.3426, "capacity_tonnes": 450,  "occupied_pct": 70, "temp_zones": ["chilled", "frozen"],                "status": "available", "contact": "+91-9876543206", "has_repair": False},
    {"id": "HUB_07", "name": "Mahindra Cold Store - Pune",     "lat": 18.5074, "lon": 73.8077, "capacity_tonnes": 350,  "occupied_pct": 40, "temp_zones": ["chilled", "cool"],                  "status": "available", "contact": "+91-9876543207", "has_repair": True},
    {"id": "HUB_08", "name": "FreezePoint - Ahmedabad",        "lat": 23.0300, "lon": 72.5800, "capacity_tonnes": 500,  "occupied_pct": 55, "temp_zones": ["chilled", "frozen", "deep_frozen"], "status": "available", "contact": "+91-9876543208", "has_repair": True},
    {"id": "HUB_09", "name": "Polar Storage - Jaipur",         "lat": 26.9000, "lon": 75.7800, "capacity_tonnes": 300,  "occupied_pct": 30, "temp_zones": ["chilled", "cool"],                  "status": "available", "contact": "+91-9876543209", "has_repair": False},
    {"id": "HUB_10", "name": "SubZero Facility - Lucknow",     "lat": 26.8500, "lon": 80.9300, "capacity_tonnes": 400,  "occupied_pct": 62, "temp_zones": ["chilled", "frozen", "deep_frozen"], "status": "available", "contact": "+91-9876543210", "has_repair": False},
    {"id": "HUB_11", "name": "ColdVault - Nagpur",             "lat": 21.1500, "lon": 79.0900, "capacity_tonnes": 250,  "occupied_pct": 35, "temp_zones": ["chilled", "frozen"],                "status": "available", "contact": "+91-9876543211", "has_repair": False},
    {"id": "HUB_12", "name": "IceBank - Indore",               "lat": 22.7200, "lon": 75.8600, "capacity_tonnes": 300,  "occupied_pct": 50, "temp_zones": ["chilled", "cool"],                  "status": "available", "contact": "+91-9876543212", "has_repair": False},
    {"id": "HUB_13", "name": "GlacierPack - Chandigarh",       "lat": 30.7300, "lon": 76.7800, "capacity_tonnes": 350,  "occupied_pct": 48, "temp_zones": ["chilled", "frozen", "deep_frozen"], "status": "available", "contact": "+91-9876543213", "has_repair": True},
    {"id": "HUB_14", "name": "CoolStore - Kochi",              "lat": 9.9300,  "lon": 76.2700, "capacity_tonnes": 400,  "occupied_pct": 60, "temp_zones": ["chilled", "frozen", "cool"],        "status": "available", "contact": "+91-9876543214", "has_repair": True},
    {"id": "HUB_15", "name": "FrostGuard - Guwahati",          "lat": 26.1500, "lon": 91.7400, "capacity_tonnes": 500,  "occupied_pct": 25, "temp_zones": ["chilled", "frozen", "deep_frozen", "cool", "ambient"], "status": "available", "contact": "+91-9876543215", "has_repair": True},
]

_hub_state = [dict(h) for h in HUBS]
_lock = threading.Lock()


# ══════════════════════════════════════════════════════════════════
#  Core Functions
# ══════════════════════════════════════════════════════════════════

def _haversine_km(lat1, lon1, lat2, lon2):
    """Calculate distance between two GPS coordinates in km."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat/2)**2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon/2)**2)
    return R * 2 * math.asin(math.sqrt(a))


def get_all_hubs():
    """Return all hubs with current status + temp zone info."""
    with _lock:
        result = []
        for h in _hub_state:
            hub = dict(h)
            # Add available capacity in tonnes
            hub["available_tonnes"] = round(
                h["capacity_tonnes"] * (1 - h["occupied_pct"] / 100), 1
            )
            # Add temp zone labels
            hub["temp_zone_labels"] = [
                TEMP_ZONES[z]["label"] for z in h.get("temp_zones", [])
                if z in TEMP_ZONES
            ]
            # Repair station flag
            hub["has_repair"] = h.get("has_repair", False)
            # Legacy compatibility
            hub["accepts"] = _zones_to_accepts(h.get("temp_zones", []))
            result.append(hub)
        return result


def _zones_to_accepts(zones):
    """Convert temp zones to legacy 'accepts' list for backward compat."""
    accepts = set()
    for product, required_zones in PRODUCT_TEMP_ZONES.items():
        if any(z in zones for z in required_zones):
            accepts.add(product)
    return sorted(accepts)


def find_nearest_hubs(lat, lon, cargo_type="dairy", top_n=3, cargo_weight_tonnes=5.0, repair_only=False):
    """
    Find nearest available hubs with intelligent matching.

    Checks:
      1. Hub status == available
      2. Temperature zone compatibility (product → required zones)
      3. Available capacity >= cargo weight
      4. Traffic-aware ETA (time-of-day, congestion zones)
      5. Optionally filter to repair stations only
    """
    with _lock:
        candidates = []
        product_key = cargo_type.lower().replace(" ", "_")

        for hub in _hub_state:
            # ── Filter 1: Must be available ──
            if hub["status"] != "available":
                continue

            # ── Filter 2: Repair station filter ──
            if repair_only and not hub.get("has_repair", False):
                continue

            # ── Filter 3: Temperature zone compatibility ──
            if not zone_matches(hub.get("temp_zones", []), product_key):
                continue

            # ── Filter 4: Capacity check (tonnes) ──
            available = hub["capacity_tonnes"] * (1 - hub["occupied_pct"] / 100)
            if available < cargo_weight_tonnes:
                continue

            # ── Distance ──
            dist = _haversine_km(lat, lon, hub["lat"], hub["lon"])

            # ── Traffic-aware ETA ──
            eta_info = compute_eta_minutes(lat, lon, hub["lat"], hub["lon"], dist)

            # ── Cost model (₹/km varies by distance tier) ──
            if dist < 50:
                rate_per_km = 45  # short haul premium
            elif dist < 200:
                rate_per_km = 38
            else:
                rate_per_km = 32  # long haul discount
            diversion_cost = round(dist * rate_per_km)

            candidates.append({
                **hub,
                "distance_km": round(dist, 1),
                "eta_minutes": eta_info["eta_minutes"],
                "avg_speed_kmph": eta_info["avg_speed_kmph"],
                "traffic_level": eta_info["traffic_level"],
                "departure_traffic": eta_info["departure_traffic"],
                "arrival_traffic": eta_info["arrival_traffic"],
                "arrival_hour": eta_info["arrival_hour"],
                "curr_zone": eta_info["curr_zone"],
                "dest_zone": eta_info["dest_zone"],
                "diversion_cost_inr": diversion_cost,
                "co2_extra_kg": round(dist * 0.25, 1),
                "available_tonnes": round(available, 1),
                "has_repair": hub.get("has_repair", False),
                "temp_zone_labels": [
                    TEMP_ZONES[z]["label"] for z in hub.get("temp_zones", [])
                    if z in TEMP_ZONES
                ],
                # Legacy compat
                "accepts": _zones_to_accepts(hub.get("temp_zones", [])),
            })

        # Sort by ETA (traffic-aware), not just distance
        candidates.sort(key=lambda h: h["eta_minutes"])
        return candidates[:top_n]


def update_hub_status(hub_id, status=None, occupied_pct=None):
    """Update hub status (for simulation)."""
    with _lock:
        for hub in _hub_state:
            if hub["id"] == hub_id:
                if status:
                    hub["status"] = status
                if occupied_pct is not None:
                    hub["occupied_pct"] = occupied_pct
                return True
    return False


def simulate_hub_dynamics():
    """Background thread: realistically fluctuate hub occupancy and status."""
    while True:
        time.sleep(15)
        with _lock:
            hub = random.choice(_hub_state)
            # Fluctuate occupancy ±3-7% (heavier traffic = more change)
            hour = datetime.now().hour
            change = random.randint(-3, 5) if 6 <= hour <= 22 else random.randint(-5, 2)
            hub["occupied_pct"] = max(10, min(97, hub["occupied_pct"] + change))
            # Occasionally toggle availability (maintenance, power outage)
            if random.random() < 0.04:
                hub["status"] = "maintenance" if hub["status"] == "available" else "available"
