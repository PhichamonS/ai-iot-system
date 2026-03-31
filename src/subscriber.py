import paho.mqtt.client as mqtt
import json
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from collections import deque
import os

# --- CONFIGURATION ---
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

DB_CONFIG = (
    f"dbname={os.getenv('DB_NAME')} "
    f"user={os.getenv('DB_USER')} "
    f"password={os.getenv('DB_PASSWORD')} "
    f"host={os.getenv('DB_HOST', 'localhost')} "
    f"port={os.getenv('DB_PORT', '5433')}"
)


# ---- DATABASE SETUP (Standard Postgres/Timescale) ----
conn = psycopg2.connect(DB_CONFIG)
cursor = conn.cursor()


# --- DUPLICATE DETECTION CACHE ---
seen_records = set()
seen_order = deque()
MAX_CACHE_SIZE = 100
BATCH_SIZE = 50
buffer = []

def transform_and_validate(raw_payload):
    try:
        data = json.loads(raw_payload)
        
        # DEBUGGING
        # print(f"RAW PAYLOAD: {data}")

        ts = data.get('timestamp')
        meter_id = data.get('meter_id')
        val_raw = data.get('consumption_kw')

        
        # 1. REJECT NULL TIMESTAMPS OR IDs
        if not ts or not meter_id:
            print(f"Dropping record: Missing timestamp or meter_id | got ts={ts}, id={meter_id}")
            return None

        # 2. DUPLICATE DETECTION (ordered cache)
        record_key = f"{ts}-{meter_id}"
        if record_key in seen_records:
            print(f"Duplicate detected: {record_key} — skipping.")
            return None

        seen_records.add(record_key)
        seen_order.append(record_key)
        if len(seen_order) > MAX_CACHE_SIZE:
            oldest = seen_order.popleft()   # evict oldest in insertion order
            seen_records.discard(oldest)

        # 3. VALIDATE & CLAMP ENERGY VALUE
        try:
            val = float(val_raw) if val_raw is not None else 0.0
            if val < 0:
                print(f"Negative value ({val}) corrected to 0 for meter {meter_id}")
                val = 0.0
        except (ValueError, TypeError):
            print(f"Invalid numeric value '{val_raw}' for meter {meter_id} — dropping.")
            return None

        return (ts, meter_id, val) # Convert to Watts

    except Exception as e:
        print(f"Critical Transformation Error: {e}")
        return None

def flush_buffer():
    global buffer
    if not buffer:
        return
    try:
        query = """
            INSERT INTO energy_data (time, meter_id, consumption_kw)
            VALUES %s
            ON CONFLICT (time, meter_id) DO NOTHING
        """
        execute_values(cursor, query, buffer)
        conn.commit()
        print(f"Flushed {len(buffer)} records to DB.")
    except Exception as e:
        print(f"DB insert error: {e}")
        conn.rollback()
    finally:
        buffer = []


def on_message(client, userdata, msg):
    global buffer
    transformed = transform_and_validate(msg.payload)

    if transformed:
        buffer.append(transformed)
        print(f"Buffered: {transformed} | buffer size: {len(buffer)}/{BATCH_SIZE}")

    if len(buffer) >= BATCH_SIZE:
        flush_buffer()


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("Connected to MQTT broker.")
        client.subscribe("data/energy/meter/#")
        print("Subscribed to data/energy/meter/#")
    else:
        print(f"Connection failed with code {reason_code}")


def on_disconnect(client, userdata, flags, reason_code, properties):
    print(f"Disconnected from broker (code={reason_code}). Flushing buffer...")
    flush_buffer()  # Save any remaining buffered records on disconnect


# --- START SUBSCRIBER ---
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect    = on_connect
client.on_message    = on_message
client.on_disconnect = on_disconnect

client.connect("broker.emqx.io", 1883, 60)

print("Subscriber listening for sensor data...")
client.loop_forever()