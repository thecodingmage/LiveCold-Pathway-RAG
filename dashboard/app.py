"""
LiveCold Dashboard - Flask Web Server
Real-time cold-chain monitoring dashboard with live map and metrics.
Subscribes to MQTT for live data and serves a web UI.
"""

import json
import threading
import logging
from datetime import datetime
from flask import Flask, render_template, jsonify, Response, request
import paho.mqtt.client as mqtt
import queue
import time
import os
import litellm
from dotenv import load_dotenv
from core.anomaly_detector import detector as anomaly_detector
from core.alert_notifier import notifier as alert_notifier

load_dotenv()

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("dashboard")

app = Flask(__name__, template_folder="templates", static_folder="static")

# ── Global State 
shipments = {}      # shipment_id -> latest state
alerts = []         # recent alerts (last 100)
diverted_shipments_saved = {}  # shipment_id -> saved_amount (prevent double-counting)
metrics = {
    "total_events": 0,
    "total_diversions": 0,
    "total_value_monitored": 0,
    "total_value_saved": 0,
    "total_co2_delta": 0,
    "high_risk_events": 0,
    "anomalies_filtered": 0,
}
sse_queue = queue.Queue(maxsize=500)

# ── Per-Shipment Routing Mode ───────────────────────────────────
# SAFETY  = nearest hub always, ignore cost (vaccines, pharma)
# BALANCED = optimize distance + cost (dairy, seafood, frozen_meat)
# ECO     = minimize CO₂, accept longer detour (fruits, flowers)
ROUTING_DEFAULTS = {
    "vaccines":        "SAFETY",
    "pharmaceuticals": "SAFETY",
    "frozen_meat":     "BALANCED",
    "dairy":           "BALANCED",
    "seafood":         "BALANCED",
    "ice_cream":       "BALANCED",
    "fruits":          "ECO",
    "flowers":         "ECO",
}
global_default_mode = "BALANCED"  # fallback for unknown cargo

# Track shipments whose routing mode was manually overridden (don't let MQTT overwrite)
routing_mode_overrides = set()  # shipment_ids that have been manually changed

# Temperature history per shipment (for prediction + charts)
temp_history = {}  # shipment_id -> list of {ts, temp}


BROKER = os.getenv("MQTT_HOST", "localhost")
PORT = 1883
mqtt_client = None  # Will be set by start_mqtt()

# Model fallback list — if one is rate-limited, try the next
GEMINI_MODELS = [
    "gemini/gemini-2.5-flash",
    "gemini/gemini-3-flash-preview",
    "gemini/gemini-2.0-flash-lite",
    "gemini/gemini-2.0-flash",
    "gemini/gemini-1.5-flash",
]
# Two API keys for rotation
API_KEYS = []
# Track which models are rate-limited (model -> cooldown_until timestamp)
model_cooldowns = {}
# Throttle: only one SOP request at a time
sop_lock = threading.Lock()
sop_in_flight = False

# ── Load SOP document at startup ───────────────────────────────
SOP_CONTEXT = ""
sop_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "watched_docs", "cold_chain_SOP.txt")
try:
    with open(sop_path, "r") as f:
        SOP_CONTEXT = f.read()
    log.info(f"📄 Loaded SOP document ({len(SOP_CONTEXT)} chars)")
except Exception as e:
    log.warning(f"Could not load SOP: {e}")

# ── SOP file watcher (detects edits and reloads context) ────────
sop_sync_status = {
    "last_modified": datetime.now().isoformat(),
    "file_size": len(SOP_CONTEXT),
    "last_mtime": os.path.getmtime(sop_path) if os.path.exists(sop_path) else 0,
    "change_count": 0,
}

def _sop_file_watcher():
    """Background thread: polls SOP file every 2s and reloads on change."""
    global SOP_CONTEXT, rag_cache
    while True:
        try:
            time.sleep(2)
            if not os.path.exists(sop_path):
                continue
            mtime = os.path.getmtime(sop_path)
            if mtime != sop_sync_status["last_mtime"]:
                with open(sop_path, "r") as f:
                    new_content = f.read()
                SOP_CONTEXT = new_content
                sop_sync_status["last_mtime"] = mtime
                sop_sync_status["last_modified"] = datetime.now().isoformat()
                sop_sync_status["file_size"] = len(new_content)
                sop_sync_status["change_count"] += 1
                # Clear RAG answer cache so stale answers aren't served
                rag_cache = {}
                log.info(f"🔄 SOP updated! Reloaded {len(new_content)} chars, cache cleared (change #{sop_sync_status['change_count']})")
                # Push SSE event to dashboard
                try:
                    sse_queue.put_nowait(json.dumps({
                        "type": "sop_update",
                        "data": {
                            "last_modified": sop_sync_status["last_modified"],
                            "file_size": sop_sync_status["file_size"],
                            "change_count": sop_sync_status["change_count"],
                        }
                    }))
                except queue.Full:
                    pass
        except Exception as e:
            log.warning(f"SOP watcher error: {e}")
            time.sleep(5)

# Load API keys dynamically from .env
from dotenv import dotenv_values
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
env_vars = dotenv_values(env_path)
for k, v in env_vars.items():
    if v and ("API_KEY" in k or "GEMINI" in k) and "HUGGING" not in k:
        if v not in API_KEYS:
            API_KEYS.append(v)

if API_KEYS:
    os.environ["GEMINI_API_KEY"] = API_KEYS[0]
    log.info(f"🔑 Loaded {len(API_KEYS)} API key(s) from .env")

# Cache recent RAG answers to avoid duplicate API calls
import requests as http_requests  # for calling Pathway RAG

rag_cache = {}  # key: alert_type_product_type -> answer

PATHWAY_RAG_URL = "http://localhost:8765/v2/answer"


def get_sop_recommendation(alert_type, product_type, shipment_id, temp):
    """Get SOP recommendation — tries Pathway RAG first, falls back to direct LLM."""
    global sop_in_flight

    # Check cache first
    cache_key = f"{alert_type}_{product_type}".lower()
    if cache_key in rag_cache:
        return rag_cache[cache_key]

    # Throttle: skip if another SOP request is already in-flight
    with sop_lock:
        if sop_in_flight:
            return "Consulting SOP..."
        sop_in_flight = True

    try:
        query = f"What should I do for a {alert_type} alert on {product_type} shipment {shipment_id} at {temp}°C? Give 3-4 action items citing SOP sections."

        # ── PRIMARY: Call Pathway RAG service (DocumentStore + LLM xPack) ──
        try:
            log.info(f"📚 Querying Pathway RAG for {alert_type}/{product_type}")
            resp = http_requests.post(
                PATHWAY_RAG_URL,
                json={"prompt": query},
                timeout=30
            )
            if resp.status_code == 200:
                data = resp.json()
                # Pathway rest_connector returns result as plain string
                if isinstance(data, str):
                    answer = data.strip()
                elif isinstance(data, dict):
                    answer = (data.get("result") or data.get("response") or "").strip()
                else:
                    answer = ""
                if answer and len(answer) > 20:
                    rag_cache[cache_key] = answer
                    log.info(f"✅ SOP via Pathway RAG for {alert_type}/{product_type}")
                    return answer
        except Exception as e:
            log.warning(f"Pathway RAG unavailable: {str(e)[:60]}, falling back to direct LLM")

        # ── FALLBACK: Direct LLM call with SOP context ──
        if not SOP_CONTEXT or not API_KEYS:
            return "SOP service starting up..."

        prompt = f"""You are a cold chain SOP assistant. Based on the following SOP document, provide a brief numbered checklist (max 4 items) for this alert.

SOP Document:
{SOP_CONTEXT}

ALERT TYPE: {alert_type}
PRODUCT: {product_type}
SHIPMENT: {shipment_id}
TEMPERATURE: {temp}°C

Provide only the most critical 3-4 action items citing SOP sections. Be very concise (one line per item)."""

        now = time.time()
        for api_key in API_KEYS:
            os.environ["GEMINI_API_KEY"] = api_key
            for model in GEMINI_MODELS:
                cooldown_key = f"{model}_{api_key[:8]}"
                if cooldown_key in model_cooldowns and now < model_cooldowns[cooldown_key]:
                    continue
                try:
                    log.info(f"🤖 Fallback: {model}")
                    response = litellm.completion(
                        model=model,
                        messages=[{"role": "user", "content": prompt}],
                        api_key=api_key,
                        timeout=30
                    )
                    answer = response.choices[0].message.content.strip()
                    rag_cache[cache_key] = answer
                    log.info(f"✅ SOP via direct LLM ({model}) for {alert_type}/{product_type}")
                    return answer
                except Exception as e:
                    err = str(e)
                    if "rate" in err.lower() or "429" in err:
                        model_cooldowns[cooldown_key] = now + 60
                        continue
                    elif "404" in err or "not found" in err.lower():
                        model_cooldowns[cooldown_key] = now + 3600
                        continue
                    else:
                        continue

        return "SOP temporarily unavailable. Retrying soon..."
    finally:
        with sop_lock:
            sop_in_flight = False


# ── MQTT Callbacks ──────────────────────────────────────────────

def on_message(client, userdata, msg):
    topic = msg.topic
    # Ignore our own divert messages
    if topic == "livecold/divert":
        return
    # ── Receive notifications from pipeline via MQTT ──────────
    if topic == "livecold/notifications":
        try:
            notification = json.loads(msg.payload.decode())
            alert_notifier._store_notification(notification)
            log.info(f"📲 Notification received: [{notification.get('type')}] {notification.get('shipment_id')}")
            try:
                sse_queue.put_nowait(json.dumps({"type": "notification", "data": notification}))
            except queue.Full:
                pass
        except Exception:
            pass
        return
    try:
        data = json.loads(msg.payload.decode())
    except Exception:
        return

    shipment_id = data.get("shipment_id", "")
    if not shipment_id:
        return

    # Initialize shipment state
    if shipment_id not in shipments:
        shipments[shipment_id] = {
            "shipment_id": shipment_id,
            "lat": None, "lon": None,
            "temp": None, "product_type": "",
            "safe_min_temp": 2, "safe_max_temp": 8,
            "compressor_status": "ON",
            "risk_probability": 0.05,
            "recommended_action": "CONTINUE",
            "origin": "", "destination": "",
            "last_update": "",
            "cargo_value_inr": 0,
            "speed_kmph": 0,
            "routing_mode": global_default_mode,  # will be updated when product_type arrives
        }

    s = shipments[shipment_id]
    s["last_update"] = datetime.now().isoformat()

    if topic == "livecold/temp":
        # Dashboard only tracks temp for history/charts — all risk calc is in pipeline
        raw_temp = data.get("temp_c")
        s["temp"] = raw_temp
        s["product_type"] = data.get("product_type", s["product_type"])
        s["safe_min_temp"] = data.get("safe_min_temp", s["safe_min_temp"])
        s["safe_max_temp"] = data.get("safe_max_temp", s["safe_max_temp"])
        s["cargo_value_inr"] = data.get("cargo_value_inr", s["cargo_value_inr"])
        metrics["total_events"] += 1

        # Track temperature history (for prediction + charts)
        if shipment_id not in temp_history:
            temp_history[shipment_id] = []
        temp_history[shipment_id].append({"ts": s["last_update"], "temp": raw_temp})
        if len(temp_history[shipment_id]) > 50:
            temp_history[shipment_id] = temp_history[shipment_id][-50:]

    elif topic == "livecold/decisions":
        # ── Pipeline decisions: the SINGLE source of truth ────────
        risk = data.get("risk_probability", 0.05)
        action = data.get("recommended_action", "CONTINUE")
        s["risk_probability"] = round(risk, 3)
        # MONITORING means "already diverted, still watching" → keep DIVERT status
        if action == "MONITORING" and s.get("recommended_action") == "DIVERT":
            pass  # keep existing DIVERT display status
        else:
            s["recommended_action"] = action
        s["temp"] = data.get("temp", s["temp"])
        s["lat"] = data.get("lat", s["lat"])
        s["lon"] = data.get("lon", s["lon"])
        s["product_type"] = data.get("product_type", s["product_type"])
        # Only update routing_mode from pipeline if NOT manually overridden
        if shipment_id not in routing_mode_overrides:
            s["routing_mode"] = data.get("routing_mode", s.get("routing_mode", "BALANCED"))
        s["cargo_value_inr"] = data.get("cargo_value_inr", s["cargo_value_inr"])
        s["origin"] = data.get("origin", s["origin"])
        s["destination"] = data.get("destination", s["destination"])
        s["speed_kmph"] = data.get("speed_kmph", s["speed_kmph"])
        s["decision_reason"] = data.get("decision_reason", "")
        s["exposure_minutes"] = data.get("exposure_minutes", 0)

        # Anomaly stats from pipeline (passed through MQTT since they're in separate process)
        metrics["anomalies_filtered"] = data.get("global_anomalies", metrics["anomalies_filtered"])
        if data.get("global_anomaly_breakdown"):
            s["anomaly_breakdown"] = data["global_anomaly_breakdown"]

        if action == "DIVERT":
            metrics["high_risk_events"] += 1
            alert = {
                "shipment_id": shipment_id,
                "temp": s["temp"],
                "risk": risk,
                "action": "DIVERT",
                "product_type": s["product_type"],
                "timestamp": s["last_update"],
                "decision_reason": s["decision_reason"],
                "sop_recommendation": "Consulting SOP..."
            }

            # Find nearest hub and prepare diversion order (but don't publish yet)
            if s.get("lat") and s.get("lon") and not s.get("divert_proposed"):
                cargo = s.get("product_type", "dairy").lower().replace(" ", "_")
                hubs = find_nearest_hubs(
                    s["lat"], s["lon"],
                    cargo_type=cargo, top_n=1
                )
                if hubs:
                    best_hub = hubs[0]
                    divert_order = {
                        "shipment_id": shipment_id,
                        "hub_lat": best_hub["lat"],
                        "hub_lon": best_hub["lon"],
                        "hub_name": best_hub["name"],
                        "hub_id": best_hub["id"],
                        "distance_km": best_hub["distance_km"],
                        "eta_minutes": best_hub["eta_minutes"],
                        "expected_loss_inr": data.get("expected_loss_inr", 0),
                        "diversion_cost_inr": data.get("diversion_cost_inr", 0),
                        "co2_delta_kg": data.get("co2_delta_kg", best_hub["distance_km"] * 0.9)
                    }
                    
                    # Store as pending instead of taking immediate action
                    s["pending_divert_order"] = divert_order
                    s["divert_proposed"] = True
                    alert["divert_hub"] = "Proposed: " + best_hub["name"]
                    alert["divert_distance"] = best_hub["distance_km"]
                    
                    # Automatically push notification to driver to ask for acceptance
                    divert_msg = {
                        "type": "DIVERT_PROPOSED",
                        "shipment_id": shipment_id,
                        "message": f"🚨 DIVERSION PROPOSED\nHigh risk detected.\nRoute to {best_hub['name']} ({best_hub['distance_km']}km away).\nPlease accept this diversion.",
                        "channel": "dashboard",
                        "timestamp": datetime.now().isoformat(),
                        "sent_to_driver": True
                    }
                    
                    if shipment_id not in driver_notifications:
                        driver_notifications[shipment_id] = []
                    driver_notifications[shipment_id].append(divert_msg)
                    
                    try:
                        mqtt_client and mqtt_client.publish(f"livecold/driver-notifications/{shipment_id}", json.dumps(divert_msg))
                    except Exception:
                        pass
                    log.info(f"⏳ Diversion proposed for {shipment_id} to {best_hub['name']}. Waiting for driver acceptance.")

            # Fetch RAG recommendation in background thread
            def fetch_sop(alert_ref, at, pt, sid, t):
                answer = get_sop_recommendation(at, pt, sid, t)
                alert_ref["sop_recommendation"] = answer

            threading.Thread(
                target=fetch_sop,
                args=(alert, "DIVERT", s["product_type"], shipment_id, s["temp"]),
                daemon=True
            ).start()

            alerts.append(alert)
            if len(alerts) > 100:
                alerts.pop(0)

            try:
                sse_queue.put_nowait(json.dumps({"type": "alert", "data": alert}))
            except queue.Full:
                pass

    elif topic == "livecold/gps":
        s["lat"] = data.get("lat")
        s["lon"] = data.get("lon")
        s["speed_kmph"] = data.get("speed_kmph", 0)
        s["origin"] = data.get("origin", s["origin"])
        s["destination"] = data.get("destination", s["destination"])

    elif topic == "livecold/reefer":
        s["compressor_status"] = data.get("compressor_status", "ON")

    elif topic == "livecold/door":
        event_type = data.get("event_type", "")
        if event_type == "door_open":
            alert = {
                "shipment_id": shipment_id,
                "temp": s.get("temp"),
                "risk": s.get("risk_probability", 0),
                "action": f"DOOR OPEN ({data.get('duration_seconds', 0)}s)",
                "product_type": s.get("product_type", ""),
                "timestamp": s["last_update"],
                "sop_recommendation": "Consulting SOP..."
            }
            
            # Fetch SOP for door events too
            def fetch_door_sop(alert_ref, pt, sid, t):
                answer = get_sop_recommendation("DOOR_OPEN", pt, sid, t)
                alert_ref["sop_recommendation"] = answer
            
            threading.Thread(
                target=fetch_door_sop,
                args=(alert, s.get("product_type", ""), shipment_id, s.get("temp", 0)),
                daemon=True
            ).start()
            
            alerts.append(alert)
            if len(alerts) > 100:
                alerts.pop(0)

    # Push state update via SSE
    try:
        sse_queue.put_nowait(json.dumps({"type": "update", "data": s}))
    except queue.Full:
        pass


def start_mqtt():
    global mqtt_client
    mqtt_client = mqtt.Client()
    mqtt_client.on_message = on_message
    try:
        mqtt_client.connect(BROKER, PORT)
        mqtt_client.subscribe("livecold/#")
        log.info("📡 Dashboard connected to MQTT broker")
        mqtt_client.loop_forever()
    except Exception as e:
        log.error(f"MQTT connection failed: {e}")


# ── Flask Routes ────────────────────────────────────────────────

# Hub management
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from core.hub_manager import get_all_hubs, find_nearest_hubs, update_hub_status, simulate_hub_dynamics

start_time = time.time()


@app.route("/")
def index():
    return render_template("index_1.html")


@app.route("/driver/<shipment_id>")
def driver_view(shipment_id):
    """Driver dashboard — mobile-friendly view for a single shipment."""
    return render_template("driver_1.html", shipment_id=shipment_id)


@app.route("/api/shipments")
def api_shipments():
    return jsonify(list(shipments.values()))


@app.route("/api/alerts")
def api_alerts():
    return jsonify(alerts[-50:])


@app.route("/api/metrics")
def api_metrics():
    m = dict(metrics)
    m["total_shipments"] = len(shipments)
    total_monitored = sum(s.get("cargo_value_inr", 0) for s in shipments.values())
    m["total_value_monitored"] = total_monitored
    # Cap cargo saved: can never exceed total value on trucks
    m["total_value_saved"] = min(m["total_value_saved"], total_monitored)
    m["active_diversions"] = sum(1 for s in shipments.values() if s.get("recommended_action") == "DIVERT")
    m["global_routing_mode"] = global_default_mode
    # Count per mode
    mode_counts = {"SAFETY": 0, "BALANCED": 0, "ECO": 0}
    for s in shipments.values():
        mode_counts[s.get("routing_mode", "BALANCED")] += 1
    m["routing_modes"] = mode_counts
    return jsonify(m)


@app.route("/api/routing-mode/<shipment_id>", methods=["POST"])
def api_set_routing_mode(shipment_id):
    """Change routing mode for a specific shipment mid-journey."""
    s = shipments.get(shipment_id)
    if not s:
        return jsonify({"error": "Shipment not found"}), 404
    data = request.get_json() or {}
    new_mode = data.get("mode", "").upper()
    if new_mode not in ("SAFETY", "BALANCED", "ECO"):
        return jsonify({"error": "Mode must be SAFETY, BALANCED, or ECO"}), 400
    old_mode = s["routing_mode"]
    s["routing_mode"] = new_mode
    # Mark as manually overridden so MQTT decisions won't revert it
    routing_mode_overrides.add(shipment_id)
    # Publish to MQTT so the pipeline also uses the new mode
    try:
        if mqtt_client:
            mqtt_client.publish("livecold/routing-mode", json.dumps({
                "shipment_id": shipment_id, "mode": new_mode
            }))
    except Exception:
        pass
    log.info(f"🔄 [{shipment_id}] Routing mode: {old_mode} → {new_mode} (manual override)")
    return jsonify({"shipment_id": shipment_id, "old_mode": old_mode, "new_mode": new_mode})


@app.route("/api/routing-mode/<shipment_id>", methods=["GET"])
def api_get_routing_mode(shipment_id):
    """Get routing mode for a specific shipment."""
    s = shipments.get(shipment_id)
    if not s:
        return jsonify({"error": "Shipment not found"}), 404
    product = s.get("product_type", "")
    pkey = product.lower().replace(" ", "_")
    return jsonify({
        "shipment_id": shipment_id,
        "routing_mode": s["routing_mode"],
        "default_for_product": ROUTING_DEFAULTS.get(pkey, global_default_mode),
        "product_type": product,
    })


@app.route("/api/global-routing", methods=["POST"])
def api_set_global_routing():
    """Set default routing mode for new shipments."""
    global global_default_mode
    data = request.get_json() or {}
    new_mode = data.get("mode", "").upper()
    if new_mode not in ("SAFETY", "BALANCED", "ECO"):
        return jsonify({"error": "Mode must be SAFETY, BALANCED, or ECO"}), 400
    old = global_default_mode
    global_default_mode = new_mode
    log.info(f"🌍 Global default routing: {old} → {new_mode}")
    return jsonify({"old": old, "new": new_mode})


@app.route("/api/history/<shipment_id>")
def api_history(shipment_id):
    """Temperature history for charts and prediction."""
    history = temp_history.get(shipment_id, [])
    # Predict 30 min ahead using last readings
    prediction = None
    if len(history) >= 5:
        temps = [h["temp"] for h in history[-10:] if h["temp"] is not None]
        if len(temps) >= 3:
            slope = (temps[-1] - temps[0]) / len(temps)
            predicted_temp = round(temps[-1] + slope * 15, 1)  # 15 intervals ≈ 30 min
            prediction = {
                "predicted_temp_30min": predicted_temp,
                "current_temp": temps[-1],
                "trend": "rising" if slope > 0.05 else "falling" if slope < -0.05 else "stable",
                "slope_per_reading": round(slope, 3),
            }
    return jsonify({
        "shipment_id": shipment_id,
        "history": history[-30:],
        "prediction": prediction,
    })


@app.route("/api/accept-diversion/<shipment_id>", methods=["POST"])
def api_accept_diversion(shipment_id):
    """Confirm a proposed diversion and officially send the redirect order."""
    s = shipments.get(shipment_id)
    if not s:
        return jsonify({"error": "Shipment not found"}), 404
        
    divert_order = s.get("pending_divert_order")
    if not divert_order:
        return jsonify({"error": "No pending diversion found"}), 400

    # Publish to MQTT to actually move the truck
    mqtt_client and mqtt_client.publish(
        "livecold/divert",
        json.dumps(divert_order)
    )
    
    # Commit changes and metrics
    s["divert_hub"] = divert_order["hub_name"]
    s["divert_distance_km"] = divert_order["distance_km"]
    s["divert_accepted"] = True
    
    # Only count each shipment's diversion ONCE
    if shipment_id not in diverted_shipments_saved:
        metrics["total_diversions"] += 1
        
        # Calculate saved value from stored order
        exp_loss = divert_order.get("expected_loss_inr", 0)
        div_cost = divert_order.get("diversion_cost_inr", 0)
        saved = max(0, int(exp_loss - div_cost))
        
        diverted_shipments_saved[shipment_id] = saved
        metrics["total_value_saved"] += saved
        metrics["total_co2_delta"] += divert_order.get("co2_delta_kg", divert_order["distance_km"] * 0.9)
        
    log.info(f"✅ Diversion ACCEPTED by driver for {shipment_id}. Truck redirecting to {divert_order['hub_name']}.")
    
    # Remove pending status
    s.pop("pending_divert_order", None)
    
    return jsonify({"status": "accepted", "shipment_id": shipment_id, "hub": divert_order["hub_name"]})

@app.route("/api/hubs")
def api_hubs():
    """All cold storage hubs with real-time status."""
    return jsonify(get_all_hubs())


@app.route("/api/nearest-hubs/<shipment_id>")
def api_nearest_hubs(shipment_id):
    """Find nearest available hubs for a shipment."""
    s = shipments.get(shipment_id)
    if not s or not s.get("lat"):
        return jsonify({"error": "Shipment not found or no GPS"}), 404

    cargo = s.get("product_type", "dairy").lower().replace(" ", "_")
    hubs = find_nearest_hubs(s["lat"], s["lon"], cargo_type=cargo, top_n=3)
    return jsonify({
        "shipment_id": shipment_id,
        "truck_lat": s["lat"],
        "truck_lon": s["lon"],
        "product_type": s.get("product_type", ""),
        "nearest_hubs": hubs,
    })


@app.route("/api/anomalies")
def api_anomalies():
    """Anomaly detection stats — global and per-shipment."""
    global_stats = anomaly_detector.get_stats()
    per_shipment = {}
    for sid in shipments:
        stats = anomaly_detector.get_shipment_stats(sid)
        if stats["total_readings"] > 0:
            last_anom = shipments[sid].get("last_anomaly")
            per_shipment[sid] = {**stats, "last_anomaly": last_anom}
    return jsonify({
        "global": global_stats,
        "per_shipment": per_shipment,
    })


@app.route("/api/stream")
def api_stream():
    """Server-Sent Events for real-time updates"""
    def generate():
        while True:
            try:
                data = sse_queue.get(timeout=5)
                yield f"data: {data}\n\n"
            except queue.Empty:
                yield f"data: {json.dumps({'type': 'ping'})}\n\n"
    return Response(generate(), mimetype="text/event-stream")


@app.route("/api/notifications")
def api_notifications():
    """Return all notifications from the alert system."""
    return jsonify(alert_notifier.get_notification_log())


@app.route("/api/notifications/<shipment_id>")
def api_notifications_by_shipment(shipment_id):
    """Return notifications for a specific shipment."""
    return jsonify(alert_notifier.get_notifications_by_shipment(shipment_id))


@app.route("/api/send-delivery-report/<shipment_id>", methods=["POST"])
def api_send_delivery_report(shipment_id):
    """Trigger delivery completion email to client."""
    s = shipments.get(shipment_id)
    if not s:
        return jsonify({"error": "Shipment not found"}), 404

    # Build report data (reuse the existing report logic)
    history = temp_history.get(shipment_id, [])
    pkey = s.get("product_type", "dairy").lower().replace(" ", "_")
    cargo_tonnes = 5
    prod_co2 = CO2_PER_TONNE.get(pkey, 3.0) * cargo_tonnes
    detour_co2 = s.get("divert_distance_km", 0) * 0.0009
    co2_saved = max(0, prod_co2 - detour_co2) if s.get("divert_hub") else 0

    report_data = {
        "shipment_id": shipment_id,
        "product_type": s.get("product_type", ""),
        "origin": s.get("origin", ""),
        "destination": s.get("destination", ""),
        "safe_range": f"{s.get('safe_min_temp', 2)}°C to {s.get('safe_max_temp', 8)}°C",
        "divert_hub": s.get("divert_hub", None),
        "carbon_credits": {
            "co2_saved_tonnes": round(co2_saved, 2),
            "credits_inr": round(co2_saved * CARBON_CREDIT_RATE_INR, 2),
        },
        "temperature_history": history[-30:],
    }

    notification = alert_notifier.send_delivery_report(shipment_id, report_data)
    return jsonify({"status": "sent", "notification": notification})


# ── Driver notification inbox (stored per shipment) ──────────────
driver_notifications = {}  # shipment_id -> list of notifications

@app.route("/api/push-notification-to-driver", methods=["POST"])
def api_push_notification_to_driver():
    """Push a notification to the driver's inbox for display on driver dashboard."""
    data = request.get_json()
    sid = data.get("shipment_id")
    ntype = data.get("type")
    if not sid:
        return jsonify({"error": "Missing shipment_id"}), 400

    # Find the matching notification from the log
    notifs = alert_notifier.get_notifications_by_shipment(sid)
    matched = None
    for n in reversed(notifs):
        if n.get("type") == ntype:
            matched = n
            break

    if not matched:
        # If no match, create a generic one
        matched = {"type": ntype, "shipment_id": sid, "message": f"{ntype} alert for {sid}",
                    "channel": "whatsapp" if "WHATSAPP" in (ntype or "") else "email",
                    "timestamp": datetime.now().isoformat()}

    matched["sent_to_driver"] = True

    if sid not in driver_notifications:
        driver_notifications[sid] = []
    driver_notifications[sid].append(matched)

    # Also publish to MQTT for real-time delivery
    try:
        mqtt_client.publish(f"livecold/driver-notifications/{sid}", json.dumps(matched))
    except Exception:
        pass

    log.info(f"📲 Pushed [{ntype}] to driver inbox for {sid}")
    return jsonify({"status": "pushed", "shipment_id": sid})


@app.route("/api/driver-notifications/<shipment_id>")
def api_driver_notifications(shipment_id):
    """Get all notifications sent to a specific driver."""
    return jsonify(driver_notifications.get(shipment_id, []))


# ── CO₂ production footprint per tonne (industry data) ──────────
CO2_PER_TONNE = {
    "vaccines": 50.0, "pharmaceuticals": 45.0,
    "frozen_meat": 27.0, "seafood": 5.4, "dairy": 3.2,
    "ice_cream": 4.0, "fruits": 0.8, "flowers": 1.2,
}
CARBON_CREDIT_RATE_INR = 1000  # ₹ per tonne CO₂


@app.route("/analytics")
def analytics_page():
    return render_template("analytics_1.html")


@app.route("/api/analytics")
def api_analytics():
    """Full analytics: carbon credits, anomaly breakdown, financials, throughput."""
    # ── Carbon Credits ────────────────────────────────────────────
    carbon = {"total_co2_avoided_kg": 0, "total_credits_inr": 0, "by_product": {}}
    for s in shipments.values():
        if s.get("recommended_action") == "DIVERT" or s.get("divert_hub"):
            pkey = s.get("product_type", "dairy").lower().replace(" ", "_")
            # Assume ~5 tonnes cargo per truck
            cargo_tonnes = 5
            prod_co2 = CO2_PER_TONNE.get(pkey, 3.0) * cargo_tonnes
            detour_co2 = s.get("divert_distance_km", 30) * 0.0009  # tonnes
            net_saved = max(0, prod_co2 - detour_co2)
            credits = net_saved * CARBON_CREDIT_RATE_INR
            carbon["total_co2_avoided_kg"] += net_saved * 1000
            carbon["total_credits_inr"] += credits
            if pkey not in carbon["by_product"]:
                carbon["by_product"][pkey] = {"co2_saved_kg": 0, "credits_inr": 0, "count": 0}
            carbon["by_product"][pkey]["co2_saved_kg"] += net_saved * 1000
            carbon["by_product"][pkey]["credits_inr"] += credits
            carbon["by_product"][pkey]["count"] += 1

    # ── Anomaly Breakdown ─────────────────────────────────────────
    anomaly_stats = {"total": metrics.get("anomalies_filtered", 0)}
    # Get latest breakdown from any stored decision data
    for s in shipments.values():
        breakdown = s.get("anomaly_breakdown")
        if breakdown:
            anomaly_stats["l1_bounds"] = breakdown.get("l1", 0)
            anomaly_stats["l2_rate"] = breakdown.get("l2", 0)
            anomaly_stats["l3_zscore"] = breakdown.get("l3", 0)
            anomaly_stats["l4_stuck"] = breakdown.get("l4", 0)
            break

    # ── Financial ─────────────────────────────────────────────────
    financial = {
        "total_value_saved": metrics.get("total_value_saved", 0),
        "total_value_monitored": sum(s.get("cargo_value_inr", 0) for s in shipments.values()),
        "total_diversions": metrics.get("total_diversions", 0),
        "total_co2_delta_kg": metrics.get("total_co2_delta", 0),
        "avg_diversion_cost": (
            metrics.get("total_co2_delta", 0) * 40 / max(metrics.get("total_diversions", 1), 1)
        ),
        "roi_percent": (
            metrics.get("total_value_saved", 0) /
            max(metrics.get("total_co2_delta", 1) * 40, 1) * 100
        ),
    }

    # ── Pipeline Throughput ───────────────────────────────────────
    pipeline = {
        "total_events": metrics.get("total_events", 0),
        "high_risk_events": metrics.get("high_risk_events", 0),
        "active_shipments": len(shipments),
        "uptime_seconds": round(time.time() - start_time),
    }

    # ── Per-shipment summary ──────────────────────────────────────
    shipment_summaries = []
    for sid, s in shipments.items():
        shipment_summaries.append({
            "shipment_id": sid,
            "product_type": s.get("product_type", ""),
            "origin": s.get("origin", ""),
            "destination": s.get("destination", ""),
            "temp": s.get("temp"),
            "risk": s.get("risk_probability", 0),
            "action": s.get("recommended_action", "CONTINUE"),
            "routing_mode": s.get("routing_mode", "BALANCED"),
            "cargo_value_inr": s.get("cargo_value_inr", 0),
            "exposure_min": s.get("exposure_minutes", 0),
            "decision_reason": s.get("decision_reason", ""),
        })

    return jsonify({
        "carbon_credits": carbon,
        "anomalies": anomaly_stats,
        "financial": financial,
        "pipeline": pipeline,
        "shipments": sorted(shipment_summaries, key=lambda x: -x["risk"]),
    })


@app.route("/api/shipment-report/<shipment_id>")
def api_shipment_report(shipment_id):
    """Downloadable report for a single shipment."""
    s = shipments.get(shipment_id)
    if not s:
        return jsonify({"error": "Shipment not found"}), 404

    history = temp_history.get(shipment_id, [])
    pkey = s.get("product_type", "dairy").lower().replace(" ", "_")

    # Carbon credit for this shipment
    cargo_tonnes = 5
    prod_co2 = CO2_PER_TONNE.get(pkey, 3.0) * cargo_tonnes
    detour_co2 = s.get("divert_distance_km", 0) * 0.0009
    co2_saved = max(0, prod_co2 - detour_co2) if s.get("divert_hub") else 0

    report = {
        "shipment_id": shipment_id,
        "product_type": s.get("product_type", ""),
        "origin": s.get("origin", ""),
        "destination": s.get("destination", ""),
        "routing_mode": s.get("routing_mode", "BALANCED"),
        "cargo_value_inr": s.get("cargo_value_inr", 0),
        "current_temp": s.get("temp"),
        "safe_range": f"{s.get('safe_min_temp', 2)}°C to {s.get('safe_max_temp', 8)}°C",
        "risk_probability": s.get("risk_probability", 0),
        "recommended_action": s.get("recommended_action", "CONTINUE"),
        "decision_reason": s.get("decision_reason", ""),
        "exposure_minutes": s.get("exposure_minutes", 0),
        "divert_hub": s.get("divert_hub", None),
        "divert_distance_km": s.get("divert_distance_km", None),
        "carbon_credits": {
            "co2_saved_tonnes": round(co2_saved, 2),
            "credits_inr": round(co2_saved * CARBON_CREDIT_RATE_INR, 2),
        },
        "temperature_history": history[-30:],
        "generated_at": datetime.now().isoformat(),
    }
    return jsonify(report)


@app.route("/sop-editor")
def sop_editor_page():
    """SOP Editor — live edit SOP documents and test RAG answers."""
    return render_template("sop_editor_1.html")


@app.route("/api/sop-content", methods=["GET"])
def api_get_sop_content():
    """Read current SOP file content."""
    try:
        with open(sop_path, "r") as f:
            content = f.read()
        return jsonify({
            "content": content,
            "file_size": len(content),
            "last_modified": sop_sync_status["last_modified"],
            "change_count": sop_sync_status["change_count"],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sop-content", methods=["POST"])
def api_save_sop_content():
    """Save edited SOP content back to file. Triggers real-time RAG update."""
    global SOP_CONTEXT, rag_cache
    data = request.get_json()
    content = data.get("content", "")
    if not content.strip():
        return jsonify({"error": "Content cannot be empty"}), 400

    try:
        with open(sop_path, "w") as f:
            f.write(content)
        # Update dashboard state immediately
        SOP_CONTEXT = content
        rag_cache = {}
        sop_sync_status["last_modified"] = datetime.now().isoformat()
        sop_sync_status["file_size"] = len(content)
        sop_sync_status["change_count"] += 1
        sop_sync_status["last_mtime"] = os.path.getmtime(sop_path)
        log.info(f"📝 SOP saved via editor ({len(content)} chars), cache cleared")
        # Push SSE
        try:
            sse_queue.put_nowait(json.dumps({
                "type": "sop_update",
                "data": {
                    "last_modified": sop_sync_status["last_modified"],
                    "file_size": sop_sync_status["file_size"],
                    "change_count": sop_sync_status["change_count"],
                }
            }))
        except queue.Full:
            pass
        return jsonify({
            "status": "saved",
            "file_size": len(content),
            "change_count": sop_sync_status["change_count"],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/rag-query", methods=["POST"])
def api_rag_query():
    """Query RAG pipeline and return the answer (for SOP editor demo)."""
    data = request.get_json()
    prompt = data.get("prompt", "")
    if not prompt.strip():
        return jsonify({"error": "Prompt cannot be empty"}), 400

    # Try Pathway RAG first
    try:
        resp = http_requests.post(
            PATHWAY_RAG_URL,
            json={"prompt": prompt},
            timeout=10
        )
        if resp.status_code == 200:
            answer_data = resp.json()
            if isinstance(answer_data, str):
                answer = answer_data.strip()
            elif isinstance(answer_data, dict):
                answer = (answer_data.get("result") or answer_data.get("response") or "").strip()
            else:
                answer = str(answer_data)
            if answer and len(answer) > 10:
                return jsonify({"answer": answer, "source": "pathway_rag"})
    except Exception as e:
        log.warning(f"Pathway RAG query failed: {str(e)[:60]}")

    # Fallback: direct LLM
    if SOP_CONTEXT and API_KEYS:
        fallback_prompt = f"""You are a cold chain SOP assistant. Based on the SOP document below, answer the question.

SOP Document:
{SOP_CONTEXT[:10000]}

Question: {prompt}

Provide a concise answer citing SOP sections."""
        for api_key in API_KEYS:
            for model in GEMINI_MODELS:
                try:
                    response = litellm.completion(
                        model=model,
                        messages=[{"role": "user", "content": fallback_prompt}],
                        api_key=api_key,
                        timeout=30,
                    )
                    answer = response.choices[0].message.content.strip()
                    return jsonify({"answer": answer, "source": "direct_llm"})
                except Exception:
                    continue

    return jsonify({"answer": "RAG service unavailable. Please start the RAG pipeline.", "source": "error"})


@app.route("/api/sop-status")
def api_sop_status():
    """SOP sync status — used by dashboard front-end indicator."""
    now = datetime.now()
    last_mod = datetime.fromisoformat(sop_sync_status["last_modified"])
    synced_ago = round((now - last_mod).total_seconds())
    return jsonify({
        "last_modified": sop_sync_status["last_modified"],
        "synced_seconds_ago": synced_ago,
        "file_size": sop_sync_status["file_size"],
        "change_count": sop_sync_status["change_count"],
    })


@app.route("/health")
def health():
    """Health check endpoint for production readiness."""
    return jsonify({
        "status": "healthy",
        "uptime_seconds": round(time.time() - start_time),
        "shipments_active": len(shipments),
        "alerts_count": len(alerts),
        "mqtt_broker": BROKER,
    })


# ── Entry Point ─────────────────────────────────────────────────

def main():
    # Start MQTT listener in background
    mqtt_thread = threading.Thread(target=start_mqtt, daemon=True)
    mqtt_thread.start()

    # Start hub dynamics simulation
    hub_thread = threading.Thread(target=simulate_hub_dynamics, daemon=True)
    hub_thread.start()

    # Start SOP file watcher
    sop_thread = threading.Thread(target=_sop_file_watcher, daemon=True)
    sop_thread.start()
    log.info("👁️ SOP file watcher started")

    print("\n" + "=" * 60)
    print("🌐 LiveCold Dashboard")
    print("=" * 60)
    print("📊 Dashboard:  http://localhost:5050")
    print("🚚 Driver View: http://localhost:5050/driver/<shipment_id>")
    print("📡 MQTT: localhost:1883")
    print("=" * 60 + "\n")

    app.run(host="0.0.0.0", port=5050, debug=False)


if __name__ == "__main__":
    main()
