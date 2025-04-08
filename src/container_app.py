import os
import ngrok
import logging

from flask import Flask, jsonify, make_response, send_file
from container_connector import container_connector
from container_snowpark import container_snowpark

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)
app.register_blueprint(container_connector, url_prefix='/container_connector')
app.register_blueprint(container_snowpark, url_prefix='/container_snowpark')

@app.route("/")
def tester():
    return send_file("container_test.html")

@app.errorhandler(404)
def resource_not_found(e):
    return make_response(jsonify(error='Not found!'), 404)

if __name__ == '__main__':
    if "NGROK_AUTHTOKEN" in os.environ:
        listener = ngrok.forward(addr=f"localhost:8001", authtoken_from_env=True)

    app.run(port=8001, host='0.0.0.0')