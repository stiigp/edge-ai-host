from flask import Flask, request, jsonify, Response
import json, queue

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

app.run(host="0.0.0.0", port=5000, threaded=True)