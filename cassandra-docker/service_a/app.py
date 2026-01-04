from flask import Flask
import time
import os

app = Flask(__name__)

@app.route("/slow")
def slow_endpoint():
    time.sleep(3)  # simulate delay
    return f"Response from {os.environ.get('HOSTNAME', 'unknown')}"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
