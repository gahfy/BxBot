import requests
from model.pairing import *
from model.currency import Currency
from decimal import Decimal
from model.trade import Trade
from datetime import datetime

# Get pairings
pairings_json = requests.get("https://bx.in.th/api/pairing/").json()
for key in pairings_json:
    current_pairing_json = pairings_json.get(key)
    current_pairing = Pairing(
        int(current_pairing_json.get('pairing_id')),
        Currency(currency_code=current_pairing_json.get('primary_currency')),
        Currency(currency_code=current_pairing_json.get('secondary_currency')),
        Decimal(current_pairing_json.get('primary_min')),
        Decimal(current_pairing_json.get('secondary_min')),
        current_pairing_json.get('active')
    )
    current_pairing.persist_in_database()

# Get trades for active pairings
pairings = get_active_pairing()
for pairing in pairings:
    json_trades = requests.get("https://bx.in.th/api/trade/?pairing=%d" % pairing.get_pairing_id()).json().get('trades')
    for json_trade in json_trades:
        pairing = Pairing(pairing.get_pairing_id())
        rate = Decimal(json_trade.get('rate'))
        amount = Decimal(json_trade.get('amount'))
        trade_date_string = "%s +07:00" % json_trade.get('trade_date')
        trade_date = datetime.strptime(trade_date_string, '%Y-%m-%d %H:%M:%S %z')
        current_trade = Trade(
            int(json_trade.get('trade_id')),
            pairing,
            rate,
            amount,
            trade_date,
            int(json_trade.get('order_id')),
            json_trade.get('trade_type')
        )
        current_trade.persist_in_database()
