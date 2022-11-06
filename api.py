from flask import Flask
from flask_cors import cross_origin
from flask import request, jsonify
from DataWorkshop.QueryRunner import QueryRunner

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False


@app.route('/',methods=['POST'])
@cross_origin()
def home():
    search = request.get_json()['query']

    runner = QueryRunner(search)

    data = runner.run()
    response = jsonify(data)

    return response


app.run()
