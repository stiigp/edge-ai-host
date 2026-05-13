from flask import Flask, jsonify, Response
from dotenv import load_dotenv
import paho.mqtt.client as mqtt
import atexit
import json
import os
import queue
import socket

load_dotenv()

PORT = int(os.getenv("PORT", "5000"))
HOST = os.getenv("HOST", "0.0.0.0")
MQTT_BROKER = os.getenv("MQTT_BROKER", "broker.emqx.io")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "edge-ai-host/gestos")
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", f"edge-ai-host-{socket.gethostname()}-{os.getpid()}")

app = Flask(__name__)
event_queue = queue.Queue()

@app.route("/health")
def health():
    return jsonify({"status": "ok!"})

def publish_gesture_event(data):
    event_queue.put(data)
    gesture = data.get("gesto", "desconhecido")
    confidence = data.get("confianca", data.get("gesto_conf"))

    if isinstance(confidence, (int, float)):
        print(f"Gesto: {gesture} ({confidence:.1f}%)")
    else:
        print(f"Gesto: {gesture}")


def decode_mqtt_payload(payload):
    text = payload.decode("utf-8")

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        data = {"gesto": text}

    if not isinstance(data, dict):
        raise ValueError("MQTT payload must be a JSON object or a gesture string.")

    if "gesto" not in data:
        raise ValueError("MQTT payload is missing the 'gesto' field.")

    return data


def create_mqtt_client():
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=MQTT_CLIENT_ID,
        protocol=mqtt.MQTTv311,
    )

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            print(f"MQTT connected to {MQTT_BROKER}:{MQTT_PORT}; subscribing to {MQTT_TOPIC}")
            client.subscribe(MQTT_TOPIC, qos=0)
        else:
            print(f"MQTT connection failed with code {reason_code}")

    def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
        if reason_code != 0:
            print(f"MQTT disconnected unexpectedly with code {reason_code}; reconnecting in the background.")

    def on_message(client, userdata, message):
        try:
            data = decode_mqtt_payload(message.payload)
        except (UnicodeDecodeError, ValueError) as exc:
            print(f"Ignoring MQTT message on {message.topic}: {exc}")
            return

        publish_gesture_event(data)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    return client


def start_mqtt():
    client = create_mqtt_client()
    client.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_start()
    print("MQTT client starting...")

    def stop_mqtt():
        client.loop_stop()
        client.disconnect()

    atexit.register(stop_mqtt)
    return client

@app.route("/stream")
def stream():
    def generate():
        while True:
            try:
                data = event_queue.get(timeout=30)
                yield f"data: {json.dumps(data)}\n\n"
            except queue.Empty:
                yield "data: {\"ping\": true}\n\n"  # keep-alive
    return Response(generate(), mimetype="text/event-stream")

@app.route("/")
def index():
    return app.send_static_file("index.html")


if __name__ == "__main__":
    mqtt_client = start_mqtt()
    app.run(host=HOST, port=PORT, threaded=True)
