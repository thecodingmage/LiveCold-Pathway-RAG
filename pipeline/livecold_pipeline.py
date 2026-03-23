"""
LiveCold Pipeline — The Single Brain
Subscribes to MQTT sensor data → runs anomaly detection → decision engine → publishes results.

This is the ONLY place where risk calculation and diversion decisions happen.
The dashboard subscribes to our output and just displays it.

Flow:
  livecold/temp  ──┐
                    ├──→ Anomaly Detection → Decision Engine → livecold/decisions
  livecold/gps  ───┘
"""

import json
import logging
import os
import paho.mqtt.client as mqtt

from decision_engine.evaluator import evaluate_shipment, get_metrics_summary
from core.anomaly_detector import detector as anomaly_detector
from core.alert_notifier import notifier as alert_notifier
from core.hub_manager import find_nearest_hubs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pipeline")

BROKER = os.getenv("MQTT_HOST", "localhost")
PORT = 1883

TEMP_TOPIC = "livecold/temp"
GPS_TOPIC = "livecold/gps"
DECISION_TOPIC = "livecold/decisions"
DIVERT_TOPIC = "livecold/divert"
ROUTING_MODE_TOPIC = "livecold/routing-mode"

# ── Per-Shipment Routing Defaults ────────────────────────────────
ROUTING_DEFAULTS = {
    "vaccines": "SAFETY",
    "pharmaceuticals": "SAFETY",
    "frozen_meat": "BALANCED",
    "dairy": "BALANCED",
    "seafood": "BALANCED",
    "ice_cream": "BALANCED",
    "fruits": "ECO",
    "flowers": "ECO",
}

# Exposure tracking: each tick ≈ 2 seconds = 0.033 minutes
EXPOSURE_INCREMENT_PER_TICK = 0.033  # ~2 seconds in minutes
EXPOSURE_DECAY_PER_TICK = 0.02       # slow decay when temp returns to safe range

shipment_state = {}
event_counter = 0
already_diverted = set()  # dedup: don't re-divert same shipment
pipeline_client = None  # MQTT client for publishing


def process_shipment(shipment_id):
    """Run decision engine on a shipment and publish the result."""
    global event_counter

    state = shipment_state.get(shipment_id)
    if not state:
        return

    # Gate: need both temp + GPS to decide
    if state.get("temp") is None or state.get("lat") is None:
        return

    # ── Call Decision Engine ──────────────────────────────────────
    final_output = evaluate_shipment(state)
    event_counter += 1

    # ── Diversion dedup: don't re-divert already diverted shipment ─
    action = final_output["recommended_action"]
    if action == "DIVERT":
        if shipment_id in already_diverted:
            final_output["recommended_action"] = "MONITORING"  # soft state
            action = "MONITORING"
        else:
            already_diverted.add(shipment_id)
    elif action == "CONTINUE" and shipment_id in already_diverted:
        already_diverted.discard(shipment_id)  # risk dropped, allow future divert

    # ── Enrich with routing mode + anomaly stats ─────────────────
    final_output["routing_mode"] = state.get("routing_mode", "BALANCED")
    final_output["product_type"] = state.get("product_type", "")
    final_output["safe_min_temp"] = state.get("safe_min_temp", 2)
    final_output["safe_max_temp"] = state.get("safe_max_temp", 8)
    final_output["cargo_value_inr"] = state.get("value_inr", 0)
    final_output["origin"] = state.get("origin", "")
    final_output["destination"] = state.get("destination", "")
    final_output["speed_kmph"] = state.get("speed_kmph", 0)
    final_output["exposure_minutes"] = state.get("exposure_minutes", 0)

    # Include GLOBAL anomaly count (so dashboard can display it across processes)
    global_anomaly_stats = anomaly_detector.get_stats()
    final_output["global_anomalies"] = global_anomaly_stats["total_anomalies"]
    final_output["global_anomaly_breakdown"] = {
        "l1": global_anomaly_stats["l1_bounds"],
        "l2": global_anomaly_stats["l2_rate"],
        "l3": global_anomaly_stats["l3_zscore"],
        "l4": global_anomaly_stats["l4_stuck"],
    }

    # ── Publish to MQTT for dashboard ─────────────────────────────
    if pipeline_client:
        pipeline_client.publish(DECISION_TOPIC, json.dumps(final_output))

    # ── Terminal logging ──────────────────────────────────────────
    action = final_output["recommended_action"]
    risk = final_output["risk_probability"]
    temp = final_output["temp"]
    sid = final_output["shipment_id"]

    if action == "DIVERT":
        net_saving = (
            final_output["expected_loss_inr"] - final_output["diversion_cost_inr"]
        )
        log.warning(
            "🚨 DIVERT | %s | temp=%.1f°C | risk=%.0f%% | saving ₹%s | %s",
            sid, temp, risk * 100,
            f"{net_saving:,.0f}",
            final_output["decision_reason"],
        )

        # ── Alert Condition 1: DIVERT → WhatsApp to driver ────────
        if state.get("lat") and state.get("lon"):
            cargo = state.get("product_type", "dairy").lower().replace(" ", "_")
            hubs = find_nearest_hubs(
                state["lat"], state["lon"],
                cargo_type=cargo, top_n=1
            )
            if hubs:
                notification = alert_notifier.send_divert_alert(
                    shipment_id=sid,
                    hub_info=hubs[0],
                    product_type=state.get("product_type", "Unknown"),
                    current_temp=temp,
                    safe_range=(state.get("safe_min_temp", 2), state.get("safe_max_temp", 8)),
                    risk_probability=risk,
                )
                # Publish notification via MQTT so dashboard can display it
                if pipeline_client and notification:
                    pipeline_client.publish("livecold/notifications", json.dumps(notification))
    else:
        log.info(
            "✅ %s | %s | temp=%.1f°C | risk=%.0f%%",
            action, sid, temp, risk * 100,
        )

    # Summary every 50 events
    if event_counter % 50 == 0:
        summary = get_metrics_summary()
        anomaly_global = anomaly_detector.get_stats()
        log.info(
            "\n╔══════════ PIPELINE METRICS (every 50 events) ══════════╗"
            "\n  Events: %d | Anomalies filtered: %d"
            "\n  Diversions: %d | Cargo saved: ₹%s"
            "\n  CO₂ delta: %.1f kg"
            "\n╚════════════════════════════════════════════════════════╝",
            summary["total_events_processed"],
            anomaly_global["total_anomalies"],
            summary["total_diversions"],
            f"{summary['cargo_value_saved_inr']:,.0f}",
            summary["total_co2_delta_kg"],
        )


def on_message(client, userdata, msg):
    topic = msg.topic
    data = json.loads(msg.payload.decode())

    # Ignore our own decision output
    if topic == DECISION_TOPIC or topic == DIVERT_TOPIC:
        return

    # ── Handle routing mode changes from dashboard ────────────────
    if topic == ROUTING_MODE_TOPIC:
        sid = data.get("shipment_id")
        new_mode = data.get("mode", "").upper()
        if sid and sid in shipment_state and new_mode in ("SAFETY", "BALANCED", "ECO"):
            old_mode = shipment_state[sid].get("routing_mode", "BALANCED")
            shipment_state[sid]["routing_mode"] = new_mode
            shipment_state[sid]["objective_mode"] = "eco" if new_mode == "ECO" else "cost"
            log.info(f"🔄 [{sid}] Routing mode updated: {old_mode} → {new_mode} (from dashboard)")
        return

    shipment_id = data.get("shipment_id")
    if not shipment_id:
        return

    # ── Init shipment state ───────────────────────────────────────
    if shipment_id not in shipment_state:
        product = data.get("product_type", "Generic")
        pkey = product.lower().replace(" ", "_")
        shipment_state[shipment_id] = {
            "shipment_id": shipment_id,
            "lat": None,
            "lon": None,
            "temp": None,
            "product_type": product,
            "value_inr": data.get("cargo_value_inr", 500000),
            "safe_min_temp": data.get("safe_min_temp", 2),
            "safe_max_temp": data.get("safe_max_temp", 8),
            "nearest_hub_distance_km": data.get("nearest_hub_distance_km", 30),
            "eta_minutes_remaining": data.get("eta_minutes_remaining", 300),
            "objective_mode": "eco" if ROUTING_DEFAULTS.get(pkey) == "ECO" else "cost",
            "routing_mode": ROUTING_DEFAULTS.get(pkey, "BALANCED"),
            "exposure_minutes": 0,
            "last_temp": None,
            "origin": data.get("origin", ""),
            "destination": data.get("destination", ""),
            "speed_kmph": 0,
        }

    # ── Temperature data ──────────────────────────────────────────
    if topic == TEMP_TOPIC:
        raw_temp = data.get("temp_c")
        product = data.get("product_type", shipment_state[shipment_id]["product_type"])
        shipment_state[shipment_id]["product_type"] = product

        # ── Anomaly Detection (4-layer filter) ────────────────────
        anomaly = anomaly_detector.check(shipment_id, raw_temp, product)
        if anomaly.is_anomaly:
            log.warning(f"🛡️ [{shipment_id}] Anomaly: {anomaly.layer} — {anomaly.reason}")

            # ── Alert Condition 2: Track anomalies for repair alert ──
            should_alert = alert_notifier.record_anomaly(shipment_id)
            if should_alert and shipment_state[shipment_id].get("lat"):
                # Find nearest repair station
                cargo = shipment_state[shipment_id].get("product_type", "dairy").lower().replace(" ", "_")
                repair_hubs = find_nearest_hubs(
                    shipment_state[shipment_id]["lat"],
                    shipment_state[shipment_id]["lon"],
                    cargo_type=cargo, top_n=1, repair_only=True
                )
                if repair_hubs:
                    anomaly_counts = alert_notifier.get_anomaly_counts()
                    notification = alert_notifier.send_repair_alert(
                        shipment_id=shipment_id,
                        anomaly_count=anomaly_counts.get(shipment_id, 3),
                        repair_hub_info=repair_hubs[0],
                        product_type=shipment_state[shipment_id].get("product_type", "Unknown"),
                    )
                    # Publish notification via MQTT
                    if pipeline_client and notification:
                        pipeline_client.publish("livecold/notifications", json.dumps(notification))

            if anomaly.layer != "L4_STUCK":
                if anomaly.corrected_temp is not None:
                    raw_temp = anomaly.corrected_temp
                else:
                    return  # discard entirely

        safe_min = shipment_state[shipment_id]["safe_min_temp"]
        safe_max = shipment_state[shipment_id]["safe_max_temp"]

        shipment_state[shipment_id]["temp"] = raw_temp
        shipment_state[shipment_id]["last_temp"] = raw_temp
        shipment_state[shipment_id]["value_inr"] = data.get("cargo_value_inr", shipment_state[shipment_id]["value_inr"])

        # Exposure tracking: increment by real-time minutes when out of range, decay when back
        if raw_temp < safe_min or raw_temp > safe_max:
            shipment_state[shipment_id]["exposure_minutes"] += EXPOSURE_INCREMENT_PER_TICK
        else:
            current = shipment_state[shipment_id]["exposure_minutes"]
            shipment_state[shipment_id]["exposure_minutes"] = max(0.0, current - EXPOSURE_DECAY_PER_TICK)

    # ── GPS data ──────────────────────────────────────────────────
    elif topic == GPS_TOPIC:
        shipment_state[shipment_id]["lat"] = data.get("lat")
        shipment_state[shipment_id]["lon"] = data.get("lon")
        shipment_state[shipment_id]["speed_kmph"] = data.get("speed_kmph", 0)
        shipment_state[shipment_id]["origin"] = data.get("origin", shipment_state[shipment_id]["origin"])
        shipment_state[shipment_id]["destination"] = data.get("destination", shipment_state[shipment_id]["destination"])
        if "eta_minutes_remaining" in data:
            shipment_state[shipment_id]["eta_minutes_remaining"] = data["eta_minutes_remaining"]

    process_shipment(shipment_id)


def start_pipeline():
    global pipeline_client
    pipeline_client = mqtt.Client(client_id="livecold-pipeline")
    pipeline_client.connect(BROKER, PORT)

    pipeline_client.subscribe(TEMP_TOPIC)
    pipeline_client.subscribe(GPS_TOPIC)
    pipeline_client.subscribe(ROUTING_MODE_TOPIC)
    pipeline_client.on_message = on_message

    log.info("🚀 LiveCold Pipeline Started")
    log.info("  📡 Subscribing: %s, %s", TEMP_TOPIC, GPS_TOPIC)
    log.info("  📤 Publishing:  %s", DECISION_TOPIC)
    log.info("  🛡️ Anomaly detection: 4 layers active")
    log.info("  🧠 Decision engine: sigmoid risk + cost optimizer")
    pipeline_client.loop_forever()


if __name__ == "__main__":
    start_pipeline()