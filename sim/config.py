# sim/config.py

import os
BROKER = os.getenv("MQTT_HOST", "localhost")
PORT = 1883

GPS_TOPIC = "livecold/gps"
TEMP_TOPIC = "livecold/temp"
REEFER_TOPIC = "livecold/reefer"
DOOR_TOPIC = "livecold/door"
DIVERT_TOPIC = "livecold/divert"

SIMULATION_INTERVAL = 2  # seconds between updates

# Indian cities (name, lat, lon)
CITIES = [
    ("Chennai", 13.0827, 80.2707),
    ("Bangalore", 12.9716, 77.5946),
    ("Delhi", 28.7041, 77.1025),
    ("Jaipur", 26.9124, 75.7873),
    ("Mumbai", 19.0760, 72.8777),
    ("Pune", 18.5204, 73.8567),
    ("Hyderabad", 17.3850, 78.4867),
    ("Kolkata", 22.5726, 88.3639),
    ("Ahmedabad", 23.0225, 72.5714),
    ("Lucknow", 26.8467, 80.9462),
    ("Bhopal", 23.2599, 77.4126),
    ("Guwahati", 26.1445, 91.7362),
    ("Patna", 25.5941, 85.1376),
    ("Indore", 22.7196, 75.8577),
    ("Nagpur", 21.1458, 79.0882),
    ("Surat", 21.1702, 72.8311),
    ("Kochi", 9.9312, 76.2673),
    ("Chandigarh", 30.7333, 76.7794),
    ("Varanasi", 25.3176, 82.9739),
    ("Ranchi", 23.3441, 85.3096),
]

NUMBER_OF_SHIPMENTS = 25
