from flask import Flask, request, jsonify, Response
from dotenv import load_dotenv
import json
import os
import queue

import ngrok

load_dotenv()

PORT = int(os.getenv("PORT", "5000"))
HOST = os.getenv("HOST", "0.0.0.0")

app = Flask(__name__)
event_queue = queue.Queue()

@app.route("/gesto", methods=["POST"])
def receber_gesto():
    data = request.json
    event_queue.put(data)
    print(f"Gesto: {data['gesto']} ({data['confianca']:.1f}%)")
    return jsonify({"ok": True})

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

def start_ngrok_tunnel():
    authtoken = os.getenv("NGROK_AUTHTOKEN")
    if not authtoken:
        print("NGROK_AUTHTOKEN is not set; running only on the local network.")
        return None

    domain = os.getenv("NGROK_DOMAIN")
    forward_options = {"authtoken": authtoken}
    if domain:
        forward_options["domain"] = domain

    listener = ngrok.forward(PORT, **forward_options)
    print(f"ngrok tunnel online: {listener.url()}")
    return listener


if __name__ == "__main__":
    start_ngrok_tunnel()
    app.run(host=HOST, port=PORT, threaded=True)
