#!/usr/bin/env python3
"""Read all DS18B20 tank sensors once, publish to MQTT, and exit (run from cron)."""
import glob
import json
import logging
import os
import sys
import uuid
from datetime import datetime

import paho.mqtt.client as mqtt

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("temps")

MQTT_PORT = 1883
MQTT_BROKER = "192.168.1.222"
MQTT_USER = "mqtt-user"
MQTT_PASS = "mqtt-user"
DATA_TOPIC = "ecl110/ecl110_data"
CONNECT_TIMEOUT = 10   # seconds
PUBLISH_TIMEOUT = 10   # seconds
READ_RETRIES = 3


def read_ds18b20(sensor_id):
    """Read temperature from a DS18B20 sensor. Returns None on failure."""
    try:
        with open(f"/sys/bus/w1/devices/{sensor_id}/w1_slave") as f:
            lines = f.read().split("\n")
        if "YES" not in lines[0].split("crc=")[1]:
            logger.warning(f"CRC check failed for sensor {sensor_id}")
            return None
        temp = float(lines[1].split(" ")[9][2:]) / 1000.0
        if temp == 85.0:  # DS18B20 power-on reset value, not a real reading
            logger.warning(f"Sensor {sensor_id} returned 85.0 (power-on value)")
            return None
        return temp
    except Exception as e:
        logger.error(f"Error reading sensor {sensor_id}: {e}")
        return None


def read_with_retry(sensor_id):
    for _ in range(READ_RETRIES):
        t = read_ds18b20(sensor_id)
        if t is not None:
            return t
    return None


def discover_sensors():
    found = [os.path.basename(p) for p in sorted(glob.glob("/sys/bus/w1/devices/28-*"))]
    if not found:
        logger.warning("No DS18B20 sensors found!")
    for s in found:
        logger.info(f"Found sensor: {s}")
    return found


def make_client():
    client_id = f"temps-{uuid.uuid4().hex[:8]}"
    try:
        return mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id)  # paho >= 2.0
    except AttributeError:
        return mqtt.Client(client_id)  # paho 1.x


def main():
    sensors = discover_sensors()
    if not sensors:
        return 1

    data = {"datetime": str(datetime.now())}
    for sensor in sensors:
        temp = read_with_retry(sensor)
        if temp is not None:
            logger.info(f"{sensor}: {temp}°C")
            data["T" + sensor[-3:]] = temp
        else:
            logger.warning(f"Sensor {sensor} returned no data")

    if len(data) == 1:
        logger.error("No valid readings, nothing to publish.")
        return 1

    payload = json.dumps(data)

    client = make_client()
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=30)
    except OSError as e:
        logger.error(f"Could not connect to broker: {e}")
        return 1

    client.loop_start()
    try:
        logger.info(f"Publishing: {payload}")
        info = client.publish(DATA_TOPIC, payload, qos=1)
        info.wait_for_publish(timeout=PUBLISH_TIMEOUT)
        if not info.is_published():
            logger.error("Publish not confirmed by broker.")
            return 1
        logger.info("Published OK")
        return 0
    finally:
        client.disconnect()
        client.loop_stop()


if __name__ == "__main__":
    sys.exit(main())

