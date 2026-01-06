from flask import Flask
import socket

app = Flask(__name__)

HOSTNAME = socket.gethostname()

@app.route("/")
def hello():
    return f"Hello from service_b! (container: {HOSTNAME})"

@app.route("/slow")
def slow():
    return f"Service B slow endpoint OK (container: {HOSTNAME})"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
