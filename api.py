import flask
from flask import request, jsonify
from DataWorkshop.QueryRunner import QueryRunner

app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET', 'POST'])
def home():
    search = request.get_json()['query']
    runner = QueryRunner(search)

    data = runner.run()
    return jsonify(data)


app.run()
