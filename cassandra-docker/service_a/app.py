from flask import Flask
import time
import os
from datetime import datetime

app = Flask(__name__)

HOSTNAME = os.environ.get('HOSTNAME', 'unknown')

@app.route("/")
def root():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"[{now}] Hello from service_a! (container: {HOSTNAME})"
    print(message)
    return message

@app.route("/slow")
def slow_endpoint():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] Slow endpoint hit in container: {HOSTNAME}")
    time.sleep(50)  # artificial delay
    response = f"[{now}] Response from {HOSTNAME} (slow)"
    print(response)
    return response

if __name__ == "__main__":
    print(f"Starting service_a in container {HOSTNAME}...")
    app.run(host="0.0.0.0", port=8000)
