from flask import Flask, jsonify
from flask_cors import CORS
from http import HTTPStatus
from utils.shooju.shooju_extractors import ShoojuExtractor
from utils.sids.continous_contract_sids import continous_contracts, cta_net_flow_to_trade

from collections import defaultdict

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

@app.route('/cta-flow-to-trade', methods=['GET'])
def cta_flow_to_trade():

    cta_flow_to_trade_per_asset = defaultdict(float)

    for asset, sid in cta_net_flow_to_trade.items():
        most_recent_ftt = round(shooju_extractor.get_series(sid)['val'].tolist()[-1]/1000, 1)

        cta_flow_to_trade_per_asset[asset] = (
            f"{"buy" if most_recent_ftt >= 0 else "sell"} {abs(most_recent_ftt)}"
        )

    return jsonify(cta_flow_to_trade_per_asset), HTTPStatus.OK

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)