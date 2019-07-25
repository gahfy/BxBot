import hashlib

import requests

from config import *
from model.order import *
from model.trade import *
from model.transaction import *
from system import log


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
pairings_json = requests.get("https://bx.in.th/api/pairing/").text
Pairing.persist_trading_from_json(pairings_json)

# Get the list of pairings with which we are trading
pairings = Pairing.get_trading_pairing()

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
    log.v("pairing: %r" % pairing)
    transaction = get_performing_transaction_for_pairing(pairing)
    log.v("Performing transaction for this pairing: %r" % transaction)
    if transaction is None:
        transaction = Transaction(
            None,
            pairing,
            True,
            datetime.now(pytz.timezone('Asia/Bangkok')),
            None,
            Decimal(balances.get(pairing.get_primary_currency().get_currency_code()).get('available')),
            Decimal(balances.get(pairing.get_secondary_currency().get_currency_code()).get('available'))
        )
        transaction.persist_in_database()
        log.v("Just persisted transaction in database: %r" % transaction)

    data = get_private_api_data(private_api_call_number)
    private_api_call_number += 1
    data["pairing"] = str(pairing.get_pairing_id())
    result = requests.post("https://bx.in.th/api/getorders/", data).json()
    json_orders = result.get('orders')
    order_ids = ""
    log.v(json_orders)
    if len(json_orders) > 0:
        comma = False
        for json_order in json_orders:
            order_ids = "%s%s%s" % (order_ids, "," if comma else "", json_order.get('order_id'))
            comma = True

    close_orders_for_transaction_with_ids_not_in(transaction, order_ids)

    orders = get_orders_for_transaction(transaction)
    if len(orders) == 0:
        balance_primary = Decimal(balances.get(pairing.get_primary_currency().get_currency_code()).get('available'))
        balance_secondary = Decimal(balances.get(pairing.get_secondary_currency().get_currency_code()).get('available'))
        total_unit_to_invest = pairing.get_total_unit_to_invest_in_per_transaction()

        primary_currency_buy_amount = balance_primary / total_unit_to_invest
        secondary_currency_sell_amount = balance_secondary / total_unit_to_invest

        last_trade = get_last_trade_for_pairing(pairing)
        primary_currency_buy_rate = last_trade.get_rate() * (
                    Decimal(1) - (pairing.get_expected_percent_win() / Decimal(200)))
        secondary_currency_sell_rate = last_trade.get_rate() * (
                    Decimal(1) + (pairing.get_expected_percent_win() / Decimal(200)))

        buy_order_data = get_private_api_data(private_api_call_number)
        private_api_call_number += 1
        buy_order_data["pairing"] = str(pairing.get_pairing_id())
        buy_order_data["type"] = "buy"
        buy_order_data["amount"] = format(primary_currency_buy_amount, '.8f')
        buy_order_data["rate"] = format(primary_currency_buy_rate, '.8f')

        sell_order_data = get_private_api_data(private_api_call_number)
        private_api_call_number += 1
        sell_order_data["pairing"] = str(pairing.get_pairing_id())
        sell_order_data["type"] = "sell"
        sell_order_data["amount"] = format(secondary_currency_sell_amount, '.8f')
        sell_order_data["rate"] = format(secondary_currency_sell_rate, '.8f')

        result_buy = requests.post("https://bx.in.th/api/order/", buy_order_data).json()
        result_sell = requests.post("https://bx.in.th/api/order/", sell_order_data).json()

        buy_order = Order(
            result_buy.get('order_id'),
            transaction,
            OrderType.BUY,
            primary_currency_buy_amount,
            primary_currency_buy_rate,
            True,
            datetime.now(pytz.timezone('Asia/Bangkok'))
        )
        buy_order.persist_in_database()

        sell_order = Order(
            result_sell.get('order_id'),
            transaction,
            OrderType.SELL,
            secondary_currency_sell_amount,
            secondary_currency_sell_rate,
            True,
            datetime.now(pytz.timezone('Asia/Bangkok'))
        )
        sell_order.persist_in_database()
    else:
        count_pending = 0
        first_pending: Optional[Order] = None
        for order in orders:
            count_pending += 1 if order.get_pending() else 0
            first_pending = order if first_pending is None else first_pending
        if count_pending == 0:
            transaction.set_performing(False)
            transaction.set_end_date(datetime.now(pytz.timezone('Asia/Bangkok')))
            transaction.persist_in_database()
        elif count_pending == 1:
            two_types = False
            previous_type = None
            for order in orders:
                if not order.get_pending():
                    previous_type = order.get_order_type() if previous_type is None else previous_type
                    if previous_type != order.get_order_type():
                        two_types = True

            if two_types:
                order = first_pending
                order.set_pending(False)
                order.set_date_close(datetime.now(pytz.timezone('Asia/Bangkok')))
                order.set_cancelled(True)
                order.persist_in_database()

                transaction.set_performing(False)
                transaction.set_end_date(datetime.now(pytz.timezone('Asia/Bangkok')))
                transaction.persist_in_database()
            else:
                last_performed_order: Optional[Order] = None
                total_amount_primary = Decimal(0)
                total_amount_secondary = Decimal(0)
                for order in orders:
                    if not order.get_cancelled() and not order.get_pending():
                        if order.get_order_type() == OrderType.BUY:
                            total_amount_primary += order.get_amount()
                            total_amount_secondary += order.get_amount() / order.get_rate()
                        else:
                            total_amount_secondary += order.get_amount()
                            total_amount_primary += order.get_amount() * order.get_rate()
                        if last_performed_order is None or datetime.timestamp(
                                last_performed_order.get_date_closed()) < datetime.timestamp(order.get_date_closed()):
                            last_performed_order = order

                order = first_pending
                order.set_pending(False)
                order.set_date_close(datetime.now(pytz.timezone('Asia/Bangkok')))
                order.set_cancelled(True)
                order.persist_in_database()

                transaction.set_performing(False)
                transaction.set_end_date(datetime.now(pytz.timezone('Asia/Bangkok')))
                transaction.persist_in_database()

                average_rate = total_amount_primary / total_amount_secondary

                if last_performed_order.get_order_type() == OrderType.BUY:
                    buy_rate = last_performed_order.get_rate() * (
                            Decimal(1) - (pairing.get_Step_gap_percent() / Decimal(100)))
                    buy_amount = last_performed_order.get_amount() * pairing.get_step_factor()

                    sell_rate = average_rate * (Decimal(1) + (pairing.get_expected_percent_win() / Decimal(100)))
                    sell_amount = total_amount_secondary

                    buy_order_data = get_private_api_data(private_api_call_number)
                    private_api_call_number += 1
                    buy_order_data["pairing"] = str(pairing.get_pairing_id())
                    buy_order_data["type"] = "buy"
                    buy_order_data["amount"] = format(buy_amount, '.8f')
                    buy_order_data["rate"] = format(buy_rate, '.8f')

                    sell_order_data = get_private_api_data(private_api_call_number)
                    private_api_call_number += 1
                    sell_order_data["pairing"] = str(pairing.get_pairing_id())
                    sell_order_data["type"] = "sell"
                    sell_order_data["amount"] = format(sell_amount, '.8f')
                    sell_order_data["rate"] = format(sell_rate, '.8f')

                    result_buy = requests.post("https://bx.in.th/api/order/", buy_order_data).json()
                    result_sell = requests.post("https://bx.in.th/api/order/", sell_order_data).json()

                    buy_order = Order(
                        result_buy.get('order_id'),
                        transaction,
                        OrderType.BUY,
                        buy_amount,
                        buy_rate,
                        True,
                        datetime.now(pytz.timezone('Asia/Bangkok'))
                    )
                    buy_order.persist_in_database()

                    sell_order = Order(
                        result_buy.get('order_id'),
                        transaction,
                        OrderType.SELL,
                        sell_amount,
                        sell_rate,
                        True,
                        datetime.now(pytz.timezone('Asia/Bangkok'))
                    )
                    sell_order.persist_in_database()
                else:
                    sell_rate = last_performed_order.get_rate() * (
                                Decimal(1) + (pairing.get_Step_gap_percent() / Decimal(100)))
                    sell_amount = last_performed_order.get_amount() * pairing.get_step_factor()

                    buy_rate = average_rate * (Decimal(1) - (pairing.get_expected_percent_win() / Decimal(100)))
                    buy_amount = total_amount_primary

                    buy_order_data = get_private_api_data(private_api_call_number)
                    private_api_call_number += 1
                    buy_order_data["pairing"] = str(pairing.get_pairing_id())
                    buy_order_data["type"] = "buy"
                    buy_order_data["amount"] = format(buy_amount, '.8f')
                    buy_order_data["rate"] = format(buy_rate, '.8f')

                    sell_order_data = get_private_api_data(private_api_call_number)
                    private_api_call_number += 1
                    sell_order_data["pairing"] = str(pairing.get_pairing_id())
                    sell_order_data["type"] = "sell"
                    sell_order_data["amount"] = format(sell_amount, '.8f')
                    sell_order_data["rate"] = format(sell_rate, '.8f')

                    result_buy = requests.post("https://bx.in.th/api/order/", buy_order_data).json()
                    result_sell = requests.post("https://bx.in.th/api/order/", sell_order_data).json()

                    buy_order = Order(
                        result_buy.get('order_id'),
                        transaction,
                        OrderType.BUY,
                        buy_amount,
                        buy_rate,
                        True,
                        datetime.now(pytz.timezone('Asia/Bangkok'))
                    )
                    buy_order.persist_in_database()

                    sell_order = Order(
                        result_buy.get('order_id'),
                        transaction,
                        OrderType.SELL,
                        sell_amount,
                        sell_rate,
                        True,
                        datetime.now(pytz.timezone('Asia/Bangkok'))
                    )
                    sell_order.persist_in_database()
