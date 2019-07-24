import hashlib
from decimal import Decimal
from time import time

import pytz
import requests

from config import *
from model.currency import Currency
from model.pairing import *
from model.trade import Trade
from model.transaction import *


def get_private_api_data(call_number: int):
    nonce = int(time()) * 1000 + call_number
    signature_clear = "%s%d%s" % (api_key, nonce, api_secret)
    signature = hashlib.sha256(signature_clear.encode()).hexdigest()
    return {
        'key': api_key,
        'nonce': "%d" % nonce,
        'signature': signature
    }


private_api_call_number = 0

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
pairings = get_trading_pairing()

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

result = requests.post("https://bx.in.th/api/balance/", get_private_api_data(private_api_call_number)).json()
private_api_call_number += 1

balances = result.get('balance')

for pairing in pairings:
    transaction = get_performing_transaction_for_pairing(pairing)
    if transaction is None:
        transaction = Transaction(
            None,
            pairing,
            True,
            datetime.now(pytz.timezone('Asia/Bangkok')),
            None
        )
        transaction.persist_in_database()
    transaction.execute()
