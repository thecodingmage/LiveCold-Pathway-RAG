"""
Reefer Unit Telemetry Simulator
Publishes compressor status, setpoint, power draw to MQTT.
"""

import time
import json
import random
from datetime import datetime
import paho.mqtt.client as mqtt

from sim.config import BROKER, PORT, REEFER_TOPIC, SIMULATION_INTERVAL
from sim.shipment_factory import generate_shipments


def main():
    client = mqtt.Client()
    client.connect(BROKER, PORT)

    shipments = generate_shipments()

    print(f"❄️  Reefer Simulator Started with {len(shipments)} shipments...")

    # Initialize reefer state per shipment
    for s in shipments:
        s["compressor_on"] = True
        s["compressor_cycles"] = random.randint(50, 200)
        s["power_draw_kw"] = round(random.uniform(1.5, 3.5), 2)

    while True:
        for shipment in shipments:
            shipment_id = shipment["shipment_id"]

            # Simulate compressor behavior
            # Occasionally toggle compressor (simulates cycling)
            if random.random() < 0.05:
                shipment["compressor_on"] = not shipment["compressor_on"]
                if shipment["compressor_on"]:
                    shipment["compressor_cycles"] += 1

            power = shipment["power_draw_kw"] if shipment["compressor_on"] else 0.0
            # Add slight variation
            power = round(power + random.uniform(-0.2, 0.2), 2) if power > 0 else 0.0

            setpoint = (shipment["safe_min_temp"] + shipment["safe_max_temp"]) / 2

            event = {
                "ts": datetime.utcnow().isoformat(),
                "shipment_id": shipment_id,
                "compressor_status": "ON" if shipment["compressor_on"] else "OFF",
                "setpoint_c": round(setpoint, 1),
                "power_draw_kw": max(0, power),
                "compressor_cycles": shipment["compressor_cycles"],
                "product_type": shipment["product_type"],
            }

            client.publish(REEFER_TOPIC, json.dumps(event))
            print("Published Reefer:", event)

        time.sleep(SIMULATION_INTERVAL)


if __name__ == "__main__":
    main()
