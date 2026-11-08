import time
import paho.mqtt.client as mqtt
import json
from datetime import datetime
import glob
import os
import logging
import uuid

# Set up logging for better debugging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MQTT_PORT = 1883
MQTT_BROKER = "192.168.1.222"
MQTT_USER = "mqtt-user"
MQTT_PASS = "mqtt-user"

LOOP_TIME = 60

data_topic = "ecl110/ecl110_data"
command_topic = "ecl110/command"

Connected = False
sensors = []
mqttc = None


def Read_DS18B20(SensorID):
    """Read temperature from a DS18B20 sensor."""
    try:
        with open("/sys/bus/w1/devices/" + SensorID + "/w1_slave") as f:
            texte = f.read()
        ligne1 = texte.split("\n")[0]
        crc = ligne1.split("crc=")[1]
        if "YES" not in crc:
            logger.warning(f"CRC check failed for sensor {SensorID}")
            return None
    except Exception as e:
        logger.error(f"Error reading sensor {SensorID}: {e}")
        return None
    ligne2 = texte.split("\n")[1]
    texte_temp = ligne2.split(" ")[9]
    return float(texte_temp[2:]) / 1000.0


def on_connect(client, userdata, flags, rc):
    global Connected
    if rc == 0:
        logger.info("Connected to MQTT Broker!")
        Connected = True
        # Subscribe here so subscriptions are renewed on reconnect
        client.subscribe(command_topic, qos=1)
        logger.info(f"Subscribed to {command_topic}")
    else:
        logger.error(f"Failed to connect, return code {rc}")
        Connected = False


def on_disconnect(client, userdata, rc):
    global Connected
    Connected = False
    if rc != 0:
        logger.warning(f"Unexpected disconnection (rc={rc}). Client will auto-reconnect.")
    else:
        logger.info("Disconnected cleanly.")


def on_message(client, userdata, msg):
    logger.info(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
    try:
        payload_command = json.loads(msg.payload.decode())
        command = payload_command.get('command', '')
        logger.info(f"Command: {command}")

        if command == 'measure':
            data = {}
            now = datetime.now()
            data["datetime"] = str(now)

            for sensor in sensors:
                temperature = Read_DS18B20(sensor)
                if temperature is not None:
                    logger.info(f"{sensor}: {temperature}°C")
                    data[("T" + sensor[-3:])] = temperature
                else:
                    logger.warning(f"Sensor {sensor} returned no data")

            data_json = json.dumps(data)
            logger.info(f"Publishing: {data_json}")
            result = client.publish(data_topic, data_json, qos=1)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                logger.error(f"Publish failed with rc={result.rc}")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in message: {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


def discover_sensors():
    """Discover all connected DS18B20 sensors."""
    found = []
    for sensor_path in glob.glob("/sys/bus/w1/devices/28-*"):
        sensor_id = os.path.basename(sensor_path)
        found.append(sensor_id)
        logger.info(f"Found sensor: {sensor_id}")
    if not found:
        logger.warning("No DS18B20 sensors found!")
    return found


def main():
    global mqttc, sensors, Connected

    # Discover sensors
    sensors = discover_sensors()

    # Set up MQTT client
    client_id = f"temps-{uuid.uuid4().hex[:8]}"
    try:
        # Try v2 API first (paho-mqtt >= 2.0)
        mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id)
    except AttributeError:
        # Fall back to v1 API
        mqttc = mqtt.Client(client_id)

    mqttc.on_connect = on_connect
    mqttc.on_disconnect = on_disconnect
    mqttc.on_message = on_message
    mqttc.username_pw_set(MQTT_USER, MQTT_PASS)

    # Enable automatic reconnection with backoff
    mqttc.reconnect_delay_set(min_delay=1, max_delay=120)

    # Initial connection with retry
    conn = False
    count = 0
    while not conn:
        try:
            mqttc.connect(MQTT_BROKER, MQTT_PORT, keepalive=120)
            conn = True
            logger.info("Initial connection established")
        except OSError as e:
            count += 1
            logger.warning(f"Network issue ({e})... retrying ({count}/10)")
            time.sleep(2)
            if count > 10:
                logger.error("Could not connect after 10 attempts. Exiting.")
                return

    # Start the network loop (handles reconnection automatically)
    mqttc.loop_start()

    # Wait for initial connection
    timeout = 30
    elapsed = 0
    while not Connected and elapsed < timeout:
        time.sleep(0.1)
        elapsed += 0.1
    if not Connected:
        logger.error("Connection timed out. Exiting.")
        mqttc.loop_stop()
        return

    logger.info("Setup complete. Waiting for commands...")

    try:
        while True:
            time.sleep(10)
            if not Connected:
                logger.warning("Connection lost. Waiting for auto-reconnect...")
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        mqttc.disconnect()
        mqttc.loop_stop()


if __name__ == "__main__":
    main()

