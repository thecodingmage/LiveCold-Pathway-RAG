"""
LiveCold Pathway MQTT Bridge
Bridges MQTT sensor events into Pathway tables for real-time stream processing.

Demonstrates:
  - pw.io.python connector for MQTT → Pathway table ingestion
  - Pathway tables: sensor_events, shipment_risk_scores
  - Rolling aggregates and risk computation via Pathway transforms
  - pw.io.csv.write for metrics output snapshots
"""

import pathway as pw
import json
import os
import threading
import time
import paho.mqtt.client as mqtt

BROKER = os.getenv("MQTT_HOST", "localhost")
PORT = 1883


# ── Pathway Schema ──────────────────────────────────────────────
class SensorEventSchema(pw.Schema):
    timestamp: str
    shipment_id: str
    sensor_id: str
    temp_c: float
    product_type: str
    safe_min_temp: float
    safe_max_temp: float
    cargo_value_inr: float


# ── MQTT → Pathway bridge using pw.io.python ────────────────────
class MQTTSubject(pw.io.python.ConnectorSubject):
    """Bridges MQTT messages into a Pathway table."""

    def run(self):
        """Subscribe to MQTT and push events to Pathway."""

        def on_message(client, userdata, msg):
            try:
                data = json.loads(msg.payload.decode())
                if "temp_c" in data and "shipment_id" in data:
                    self.next(
                        timestamp=data.get("ts", ""),
                        shipment_id=data["shipment_id"],
                        sensor_id=data.get("sensor_id", "PALLET_1"),
                        temp_c=data["temp_c"],
                        product_type=data.get("product_type", "Generic"),
                        safe_min_temp=data.get("safe_min_temp", 2.0),
                        safe_max_temp=data.get("safe_max_temp", 8.0),
                        cargo_value_inr=data.get("cargo_value_inr", 500000.0),
                    )
            except Exception as e:
                print(f"MQTT parse error: {e}")

        client = mqtt.Client()
        client.on_message = on_message
        client.connect(BROKER, PORT)
        client.subscribe("livecold/temp")
        print(f"📡 Pathway MQTT bridge connected to {BROKER}:{PORT}")
        client.loop_forever()


def build_pathway_pipeline():
    """Build and run the Pathway streaming pipeline with MQTT input."""

    print("\n" + "=" * 60)
    print("LiveCold Pathway MQTT Bridge")
    print("=" * 60)

    # ── Step 1: MQTT → Pathway table ────────────────────────────
    mqtt_subject = MQTTSubject()
    sensor_events = pw.io.python.read(
        mqtt_subject,
        schema=SensorEventSchema,
    )
    print("  ✓ MQTT → Pathway sensor_events table")

    # ── Step 2: Anomaly Detection (filter garbage data) ───────────
    anomaly_count = {"total": 0}  # Track for dashboard

    @pw.udf
    def is_valid_reading(temp: float) -> bool:
        """Filter physically impossible readings."""
        if temp < -50 or temp > 80:
            return False  # impossible for cold chain
        return True

    @pw.udf
    def anomaly_label(temp: float) -> str:
        if temp < -50 or temp > 80:
            return "IMPOSSIBLE_TEMP"
        return "OK"

    # Filter out bad readings
    valid_events = sensor_events.filter(
        is_valid_reading(sensor_events.temp_c)
    )
    print("  ✓ Anomaly detection filter (rejects temp < -50 or > 80)")

    # ── Step 3: Compute temperature excursions ──────────────────
    @pw.udf
    def is_excursion(temp: float, safe_min: float, safe_max: float) -> bool:
        return temp < safe_min or temp > safe_max

    @pw.udf
    def excursion_severity(temp: float, safe_min: float, safe_max: float) -> float:
        if temp > safe_max:
            return min((temp - safe_max) / safe_max, 1.0)
        elif temp < safe_min:
            return min((safe_min - temp) / abs(safe_min) if safe_min != 0 else 0.5, 1.0)
        return 0.0

    @pw.udf
    def compute_risk_score(severity: float, cargo_value: float) -> float:
        """Simple risk: severity * normalized cargo value."""
        value_factor = min(cargo_value / 2500000.0, 1.0)  # normalize by max value
        return round(min(severity * 0.7 + value_factor * 0.3, 1.0), 3)

    # Enriched sensor events with excursion detection
    enriched_events = valid_events.select(
        timestamp=valid_events.timestamp,
        shipment_id=valid_events.shipment_id,
        temp_c=valid_events.temp_c,
        product_type=valid_events.product_type,
        cargo_value_inr=valid_events.cargo_value_inr,
        is_excursion=is_excursion(
            valid_events.temp_c,
            valid_events.safe_min_temp,
            valid_events.safe_max_temp,
        ),
        severity=excursion_severity(
            valid_events.temp_c,
            valid_events.safe_min_temp,
            valid_events.safe_max_temp,
        ),
    )
    print("  ✓ Temperature excursion detection")

    # ── Step 4: Per-shipment risk scores ────────────────────────

    risk_scores = enriched_events.select(
        shipment_id=enriched_events.shipment_id,
        product_type=enriched_events.product_type,
        temp_c=enriched_events.temp_c,
        risk_score=compute_risk_score(
            enriched_events.severity,
            enriched_events.cargo_value_inr,
        ),
        is_excursion=enriched_events.is_excursion,
    )
    print("  ✓ Per-shipment risk scoring")

    # ── Step 4: Output to CSV snapshots ─────────────────────────
    os.makedirs("./metrics", exist_ok=True)

    pw.io.csv.write(risk_scores, "./metrics/pathway_risk_scores.csv")
    print("  ✓ Output: ./metrics/pathway_risk_scores.csv")

    print(f"\n{'=' * 60}")
    print("🚀 Pathway MQTT Bridge Running")
    print(f"   📡 Ingesting from MQTT {BROKER}:{PORT}")
    print("   📊 Computing risk scores in real-time")
    print("   📁 Writing to ./metrics/pathway_risk_scores.csv")
    print("=" * 60 + "\n")

    # Run Pathway — blocks until interrupted
    pw.run()


if __name__ == "__main__":
    build_pathway_pipeline()
