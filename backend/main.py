import json
from flask import Flask, jsonify
from flask_cors import CORS
from http import HTTPStatus
from utils.shooju.shooju_extractors import ShoojuExtractor
from utils.sids.continous_contract_sids import continous_contracts

# Initialize the extractor to get data with Shooju
shooju_extractor = ShoojuExtractor()

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

@app.route('/ping')
def ping():
    response = {"message": "pong"}
    return jsonify(response), HTTPStatus.OK

@app.route('/continous_contract/<asset>', methods=['GET'])
def continous_contract(asset):
    try:
        series = shooju_extractor.get_series(continous_contracts[asset]).set_index('dt')[['val']].loc["2024-01-01":]
    except Exception as e:
        return jsonify({"error": str(e)}), HTTPStatus.NOT_FOUND

    data = series.to_json(orient='index', date_format='iso')

    response = {"data": data}
    return jsonify(response), HTTPStatus.OK

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)