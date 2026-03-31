# AI-IOT-SYSTEM

An IoT energy monitoring pipeline that collects sensor data via MQTT, stores it in TimescaleDB, and visualizes it in Grafana.

## Architecture

```
IoT Sensor → producer.py → MQTT Broker → subscriber.py → TimescaleDB → Grafana
```

## Project Structure

```
AI-IOT-SYSTEM/
├── data/                   # Data files
├── src/
│   ├── producer.py         # Publishes sensor data to MQTT
│   ├── subscriber.py       # Subscribes to MQTT and writes to TimescaleDB
│   └── db_setup.py         # One-time DB schema setup
├── notebooks/
│   └── notebooks.ipynb     # Data exploration
├── .env                    # Environment variables (never commit)
├── docker-compose.yml      # TimescaleDB + Grafana containers
├── requirements.txt
└── README.md
```

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- Python 3.8+

---

## Setup (Run Once)

### 1. Clone the repo

```bash
git clone <your-repo-url>
cd ai-iot-system
```

### 2. Create `.env` file in project root

```
DB_HOST=localhost
DB_PORT=5433
DB_NAME=energy_db
DB_USER=postgres
DB_PASSWORD=yourpassword
```

> ⚠️ Never commit `.env` to Git. Make sure it's in `.gitignore`.

### 3. Install Python dependencies

```bash
pip install psycopg2-binary python-dotenv paho-mqtt sqlalchemy
```

### 4. Start Docker containers

```bash
docker-compose up -d
```

Verify both containers are running:

```bash
docker ps
# timescaledb → port 5433
# grafana     → port 3000
```

### 5. Create the database and enable TimescaleDB extension

Connect into the TimescaleDB container:

```bash
docker exec -it ai-iot-system-timescaledb-1 psql -U postgres
```

Inside the psql prompt:

```sql
CREATE DATABASE energy_db;
\c energy_db
CREATE EXTENSION IF NOT EXISTS timescaledb;
\q
```

### 6. Run the DB setup script

```bash
python src/db_setup.py
# Output: Hypertable created successfully!
```

Verify the table was created:

```bash
docker exec -it ai-iot-system-timescaledb-1 psql -U postgres -d energy_db -c "\dt"
```

---

## Running the System

Open **two terminals** from the project root:

```bash
# Terminal 1 — Subscriber (listens for MQTT and writes to TimescaleDB)
python src/subscriber.py

# Terminal 2 — Producer (publishes simulated sensor data)
python src/producer.py
```

Verify data is being written to the database:

```bash
docker exec -it ai-iot-system-timescaledb-1 psql -U postgres -d energy_db -c "SELECT * FROM energy_data LIMIT 10;"
```

Check the time range of your data:

```bash
docker exec -it ai-iot-system-timescaledb-1 psql -U postgres -d energy_db -c "SELECT MIN(time), MAX(time), COUNT(*) FROM energy_data;"
```

---

## Grafana Visualization

### 1. Open Grafana

Go to [http://localhost:3000](http://localhost:3000)

```
Username: admin
Password: admin
```

### 2. Add TimescaleDB as a Data Source

1. Left sidebar → **Connections** → **Data Sources**
2. Click **Add new data source**
3. Select **PostgreSQL**
4. Fill in the connection details:

```
Host:        timescaledb:5432
Database:    energy_db
User:        postgres
Password:    yourpassword
TLS/SSL:     disable
TimescaleDB: ON 
```

5. Click **Save & Test** → should show ✅

### 3. Create a Dashboard

1. Left sidebar → **Dashboards** → **New Dashboard**
2. Click **Add visualization**
3. Select your **PostgreSQL** data source
4. Switch to **Code** mode at the bottom
5. Paste the query:

```sql
SELECT
    time AS "time",
    meter_id,
    consumption_watts
FROM energy_data
WHERE
    $__timeFilter(time)
ORDER BY time;
```

### 4. Configure the Panel

| Setting | Value |
|---|---|
| Panel type | Time series |
| Title | Energy Consumption |
| Unit | Watt (W) |
| Legend Mode | Table |
| Legend Values | Last, Max, Mean |

6. Set the time range (top right) to match your data, e.g. `Last 6 hours`
7. Click 💾 **Save dashboard**

---

## Daily Usage

Every time you want to run the system:

```bash
# 1. Start Docker containers
docker-compose up -d

# 2. Terminal 1 — Start subscriber
python src/subscriber.py

# 3. Terminal 2 — Start producer
python src/producer.py

# 4. Open Grafana
# http://localhost:3000
```

To stop everything:

```bash
docker-compose down
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `docker` not recognized | Start Docker Desktop first |
| Port conflict on 5433 | Run `docker-compose down -v` then `docker-compose up -d` |
| Grafana can't connect to DB | Use `timescaledb:5432` as host (not `localhost:5433`) |
| Graph is empty | Check time range in Grafana matches your data |
| `.env` not loading | Make sure `.env` is in project root, not inside `src/` |
| No data in DB | Make sure both producer and subscriber are running |
| `Hypertable` error | Ensure `CREATE EXTENSION timescaledb` was run inside the container first |
