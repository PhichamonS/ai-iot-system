import pandas as pd
import paho.mqtt.client as mqtt
import json
import time

# --- CONFIGURATION ---
BROKER = "broker.emqx.io"  # Public test broker
PORT = 1883
TOPIC_PREFIX = "data/energy/meter"
DATA_PATH = "data/LD2011_2014.txt" 

# 1. Setup MQTT Client 
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        print("Connected to MQTT Broker!")
    else:
        print(f"Failed to connect, return code {rc}")

client.on_connect = on_connect
client.connect(BROKER, PORT, 60)
client.loop_start() # Start a background thread to handle network traffic

# 2. Load and Prepare Data
print("Reading dataset (this might take a moment)...")
# Note: The UCI dataset uses ';' and decimal commas in some versions
df = pd.read_csv(DATA_PATH, sep=';', decimal=',', low_memory=False)
df.rename(columns={df.columns[0]: 'timestamp'}, inplace=True)

# 3. Simulation Loop
try:
    print("Starting Simulation. Press Ctrl+C to stop.")
    for index, row in df.iterrows():
        # Let's simulate for the first 5 clients to keep it readable
        for client_id in ["MT_001", "MT_002", "MT_003", "MT_004", "MT_005"]:
            payload = {
                "meter_id": client_id,
                "timestamp": row['timestamp'],
                "consumption_kw": float(row[client_id])
            }
            
            # Publish to a specific topic for each meter
            topic = f"{TOPIC_PREFIX}/{client_id}"
            client.publish(topic, json.dumps(payload), qos=1)
            
        print(f"Sent data for timestamp: {row['timestamp']}")
        time.sleep(1) # Simulate a 1-second interval (Real data is 15-min)

except KeyboardInterrupt:
    print("\nStopping simulation...")
    client.loop_stop()
    client.disconnect()
    