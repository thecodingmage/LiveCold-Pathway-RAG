# sim/gps_simulator.py

import time
import json
import threading
from datetime import datetime
import paho.mqtt.client as mqtt

from sim.config import BROKER, PORT, GPS_TOPIC, DIVERT_TOPIC, SIMULATION_INTERVAL
from sim.shipment_factory import generate_shipments


def move_towards(current, target, step):
    if abs(target - current) < step:
        return target
    return current + step if target > current else current - step


# Track active diversions: shipment_id -> {hub_lat, hub_lon, hub_name}
active_diversions = {}
diversion_lock = threading.Lock()


def on_divert_message(client, userdata, msg):
    """Handle diversion orders from the dashboard."""
    try:
        data = json.loads(msg.payload)
        sid = data.get("shipment_id")
        if sid and data.get("hub_lat") and data.get("hub_lon"):
            with diversion_lock:
                active_diversions[sid] = {
                    "hub_lat": data["hub_lat"],
                    "hub_lon": data["hub_lon"],
                    "hub_name": data.get("hub_name", "Unknown Hub"),
                }
            print(f"🔀 DIVERTING {sid} → {data.get('hub_name', 'Hub')} "
                  f"({data['hub_lat']:.2f}, {data['hub_lon']:.2f})")
    except Exception as e:
        print(f"⚠️ Divert parse error: {e}")


def main():
    client = mqtt.Client()
    client.connect(BROKER, PORT)

    # Subscribe to diversion orders
    client.subscribe(DIVERT_TOPIC)
    client.message_callback_add(DIVERT_TOPIC, on_divert_message)
    client.loop_start()

    shipments = generate_shipments()

    print(f"🚚 GPS Simulator Started with {len(shipments)} shipments...")
    print(f"📡 Listening for diversions on {DIVERT_TOPIC}")

    while True:
        for shipment in shipments:
            sid = shipment["shipment_id"]

            if shipment.get("arrived"):
                continue

            # Check if this shipment is diverted
            with diversion_lock:
                diversion = active_diversions.get(sid)

            if diversion:
                # Move toward the hub instead of the original destination
                target_lat = diversion["hub_lat"]
                target_lon = diversion["hub_lon"]
                dest_name = diversion["hub_name"]

                # Check if arrived at hub (within ~1km)
                dist = ((shipment["current_lat"] - target_lat)**2 +
                        (shipment["current_lon"] - target_lon)**2) ** 0.5
                if dist < 0.01:  # ~1km
                    if not shipment.get("arrived_log_printed"):
                        print(f"✅ {sid} ARRIVED at {dest_name}")
                        shipment["arrived_log_printed"] = True
                    shipment["eta_minutes_remaining"] = 0
                    shipment["speed_kmph"] = 0
                    shipment["arrived"] = True
                    
                    # Make sure the final position is exactly the hub
                    shipment["current_lat"] = target_lat
                    shipment["current_lon"] = target_lon
                    
                    # Do NOT delete from active_diversions so pipeline knows it's still diverted
                    # Do NOT reset ETA to 30 minutes
                    pass
            else:
                # Normal: move toward original destination
                target_lat = shipment["end_lat"]
                target_lon = shipment["end_lon"]
                dest_name = shipment["destination"]

            # Movement step based on speed
            step_size = shipment["speed_kmph"] / 10000

            shipment["current_lat"] = move_towards(
                shipment["current_lat"], target_lat, step_size
            )
            shipment["current_lon"] = move_towards(
                shipment["current_lon"], target_lon, step_size
            )

            # Decrease ETA
            shipment["eta_minutes_remaining"] = max(
                0, shipment["eta_minutes_remaining"] - (SIMULATION_INTERVAL / 60)
            )

            event = {
                "ts": datetime.utcnow().isoformat(),
                "shipment_id": sid,
                "lat": round(shipment["current_lat"], 6),
                "lon": round(shipment["current_lon"], 6),
                "speed_kmph": shipment["speed_kmph"],
                "origin": shipment["origin"],
                "destination": dest_name,
                "eta_minutes_remaining": round(shipment["eta_minutes_remaining"], 1),
                "product_type": shipment["product_type"],
                "diverted": diversion is not None,
            }

            client.publish(GPS_TOPIC, json.dumps(event))
            print("Published GPS:", event)

        time.sleep(SIMULATION_INTERVAL)


if __name__ == "__main__":
    main()
