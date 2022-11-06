from flask import Flask


def create_app():
    app = Flask(__name__)

    app.config['CORS_HEADERS'] = 'application/json'
    app.config["DEBUG"] = True
    app.config["JSON_SORT_KEYS"] = False


    return app