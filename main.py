from flask import Flask, request, jsonify, Response
from dotenv import load_dotenv
import atexit
import json
import os
import queue
import socket
import shutil
import subprocess
import threading
import time

load_dotenv()

PORT = int(os.getenv("PORT", "5000"))
HOST = os.getenv("HOST", "0.0.0.0")
ENABLE_LOCALTUNNEL = os.getenv("ENABLE_LOCALTUNNEL", "true").lower() in ("1", "true", "yes", "on")

app = Flask(__name__)
event_queue = queue.Queue()

@app.route("/health")
def health():
    return jsonify({"status": "ok!"})

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

def wait_for_local_server(timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", PORT), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.25)

    return False


def get_localtunnel_command():
    local_bin = os.path.join(os.getcwd(), "node_modules", ".bin", "lt.cmd")
    if os.path.exists(local_bin):
        return [local_bin]

    local_bin = os.path.join(os.getcwd(), "node_modules", ".bin", "lt")
    if os.path.exists(local_bin):
        return [local_bin]

    npx = shutil.which("npx") or shutil.which("npx.cmd")
    if npx:
        return [npx, "--yes", "localtunnel"]

    return None


def start_localtunnel():
    if not ENABLE_LOCALTUNNEL:
        print("localtunnel is disabled; running only on the local network.")
        return None

    if not wait_for_local_server():
        print(f"localtunnel was not started because http://127.0.0.1:{PORT} did not respond.")
        return None

    command = get_localtunnel_command()
    if command is None:
        print("localtunnel was not started because Node.js/npm is not installed.")
        return None

    command.extend(["--port", str(PORT), "--local-host", "127.0.0.1", "--host", "http://loca.lt"])

    subdomain = os.getenv("LOCALTUNNEL_SUBDOMAIN")
    if subdomain:
        command.extend(["--subdomain", subdomain])

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    def pipe_output():
        if process.stdout is None:
            return

        for line in process.stdout:
            print(f"localtunnel: {line}", end="")

    threading.Thread(target=pipe_output, daemon=True).start()

    def report_exit():
        exit_code = process.wait()
        print(f"localtunnel stopped with exit code {exit_code}.")

    threading.Thread(target=report_exit, daemon=True).start()

    def stop_localtunnel():
        if process.poll() is None:
            process.terminate()

    atexit.register(stop_localtunnel)
    print("localtunnel starting...")
    return process


if __name__ == "__main__":
    tunnel_thread = threading.Timer(1.0, start_localtunnel)
    tunnel_thread.daemon = True
    tunnel_thread.start()
    app.run(host=HOST, port=PORT, threaded=True)
