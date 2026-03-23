import time
import json
import random
from datetime import datetime
import paho.mqtt.client as mqtt

from sim.config import BROKER, PORT, TEMP_TOPIC, SIMULATION_INTERVAL
from sim.shipment_factory import generate_shipments


def main():
    client = mqtt.Client()
    client.connect(BROKER, PORT)

    shipments = generate_shipments()

    print(f"🌡  Temperature Simulator Started with {len(shipments)} shipments...")

    # Assign a fixed nearest-hub distance per shipment (used by pipeline for diversion cost)
    for shipment in shipments:
        shipment["nearest_hub_distance_km"] = random.randint(10, 80)

    while True:
        for shipment in shipments:

            shipment_id = shipment["shipment_id"]
            mode        = shipment["temp_mode"]
            base_temp   = shipment["base_temp"]
            safe_min    = shipment["safe_min_temp"]
            safe_max    = shipment["safe_max_temp"]

            if mode == "stable":
                temp = base_temp + random.uniform(-0.5, 0.5)

            elif mode == "drift":
                drift_rate = 0.15 if shipment["sensitivity"] == "HIGH" else 0.08
                new_temp = base_temp + drift_rate

                # ── Cap drift before it goes absurd ──────────────────────────
                # If temp has crept more than 12°C above the safe ceiling,
                # simulate a refrigeration repair: snap back to safe range
                # and return to stable mode.
                if new_temp > safe_max + 12:
                    print(f"🔧 [{shipment_id}] Refrigeration repaired — resetting to stable mode")
                    new_temp = safe_max - 1           # just inside safe window
                    shipment["temp_mode"] = "stable"  # switch back
                # ─────────────────────────────────────────────────────────────

                temp = new_temp
                shipment["base_temp"] = temp

            else:
                temp = base_temp

            # ── Inject anomalous readings for demo (~9% of readings) ────
            anomaly_type = None
            if random.random() < 0.03:
                # L1: Hardware glitch — impossible value
                temp = random.choice([999.0, -999.0, 500.0, -200.0])
                anomaly_type = "L1_GLITCH"
            elif random.random() < 0.04:
                # L2: Sudden spike — sensor fault
                temp = temp + random.choice([20, -20, 15, -15])
                anomaly_type = "L2_SPIKE"
            elif random.random() < 0.02:
                # L3: Statistical outlier — mild but detectable
                temp = temp + random.uniform(6, 10) * random.choice([1, -1])
                anomaly_type = "L3_OUTLIER"

            if anomaly_type:
                print(f"⚡ [{shipment_id}] Injected {anomaly_type}: {temp}°C")

            event = {
                "ts":                       datetime.utcnow().isoformat(),
                "shipment_id":              shipment_id,
                "sensor_id":                "PALLET_1",
                "temp_c":                   round(temp, 2),
                "product_type":             shipment["product_type"],
                "safe_min_temp":            safe_min,
                "safe_max_temp":            safe_max,
                "cargo_value_inr":          shipment["cargo_value_inr"],
                "nearest_hub_distance_km":  shipment["nearest_hub_distance_km"],
            }

            client.publish(TEMP_TOPIC, json.dumps(event))
            print("Published Temp:", event)

        time.sleep(SIMULATION_INTERVAL)


if __name__ == "__main__":
    main()