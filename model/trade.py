from __future__ import annotations
from model.pairing import Pairing
from decimal import Decimal
from datetime import datetime
from system.database import cursor, conn
import pytz


class Trade(object):
    __trade_id: int = None
    __pairing: Pairing = None
    __rate: Decimal = None
    __amount: Decimal = None
    __trade_date: datetime = None
    __order_id: int = None
    __trade_type: str = None

    def __init__(
            self: Trade,
            trade_id: int = None,
            pairing: Pairing = None,
            rate: Decimal = None,
            amount: Decimal = None,
            trade_date: datetime = None,
            order_id: int = None,
            trade_type: str = None
    ):
        if (pairing is not None
                or rate is not None
                or amount is not None
                or trade_date is not None
                or order_id is not None
                or trade_type is not None
        ):
            self.set_trade_id(trade_id)
            self.set_pairing(pairing)
            self.set_rate(rate)
            self.set_amount(amount)
            self.set_trade_date(trade_date)
            self.set_order_id(order_id)
            self.set_trade_type(trade_type)
        elif trade_id is not None:
            self.retrieve_from_database(trade_id)
        else:
            raise ValueError('All attributes are None.')

    def set_trade_id(self: Trade, trade_id: int) -> Trade:
        if trade_id is not None:
            if trade_id > 0:
                self.__trade_id = trade_id
            else:
                raise ValueError('trade_id must be greater than 0.')
        else:
            raise ValueError('trade_id must be not None.')
        return self

    def set_pairing(self: Trade, pairing: Pairing) -> Trade:
        if pairing is not None:
            self.__pairing = pairing
        else:
            raise ValueError('pairing must be not None.')
        return self

    def set_rate(self: Trade, rate: Decimal) -> Trade:
        if rate is not None:
            self.__rate = rate
        else:
            raise ValueError('rate must be not None.')
        return self

    def set_amount(self: Trade, amount: Decimal) -> Trade:
        if amount is not None:
            self.__amount = amount
        else:
            raise ValueError('amount must be not None.')
        return self

    def set_trade_date(self: Trade, trade_date: datetime) -> Trade:
        if trade_date is not None:
            self.__trade_date = trade_date
        else:
            raise ValueError('trade_date must be not None.')
        return self

    def set_order_id(self: Trade, order_id: int) -> Trade:
        if order_id is not None:
            if order_id > 0:
                self.__order_id = order_id
            else:
                raise ValueError('order_id must be greater than 0')
        else:
            raise ValueError('order_id must be not None.')
        return self

    def set_trade_type(self: Trade, trade_type: str) -> Trade:
        if trade_type is not None:
            if len(trade_type) > 0:
                self.__trade_type = trade_type
            else:
                raise ValueError('trade_type must be not empty.')
        else:
            raise ValueError('trade_type must be not None.')
        return self

    def retrieve_from_database(self: Trade, trade_id: int):
        query = """
            SELECT `trade_id`,
                   `pairing_id`,
                   `rate`,
                   `amount`,
                   `trade_date`,
                   `order_id`,
                   `trade_type`
            FROM `trade`
            WHERE `trade_id` = %s
        """
        cursor.execute(query, (trade_id,))
        result = cursor.fetchone()
        if result is not None:
            self.set_trade_id(result[0])
            self.set_pairing(Pairing(result[1]))
            self.set_rate(result[2])
            self.set_amount(result[3])
            self.set_trade_date(datetime.fromtimestamp(result[4], pytz.timezone('Asia/Bangkok')))
            self.set_order_id(result[5])
            self.set_trade_type(result[6])
        else:
            raise ValueError('No trade found with id %d' % trade_id)

    def persist_in_database(self: Trade):
        result = None
        try:
            result = Trade(self.__trade_id)
        except ValueError:
            pass
        if result is None:
            query_insert = """
                            INSERT INTO `trade` (
                                `trade_id`,
                                `pairing_id`,
                                `rate`,
                                `amount`,
                                `trade_date`,
                                `order_id`,
                                `trade_type`
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """

            cursor.execute(query_insert, (
                self.__trade_id,
                self.__pairing.get_pairing_id(),
                format(self.__rate, '.8f'),
                format(self.__amount, '.8f'),
                datetime.timestamp(self.__trade_date),
                self.__order_id,
                self.__trade_type
            ))
            conn.commit()
            print("Inserting new trade")
        else:
            if self != result:
                query_update = """
                                UPDATE `trade`
                                SET `trade_id` = %s,
                                    `pairing_id` = %s,
                                    `rate` = %s,
                                    `amount` = %s,
                                    `trade_date` = %s,
                                    `order_id` = %s,
                                    `trade_type` = %s
                                WHERE `trade_id` = %s
                                """

                cursor.execute(query_update, (
                    self.__trade_id,
                    self.__pairing.get_pairing_id(),
                    format(self.__rate, '.8f'),
                    format(self.__amount, '.8f'),
                    datetime.timestamp(self.__trade_date),
                    self.__order_id,
                    self.__trade_type,
                    self.__trade_id
                ))
                conn.commit()
                print("Updating trade")

    def __eq__(self: Trade, compare: Trade) -> bool:
        return (self.__trade_id == compare.__trade_id
                and self.__pairing.get_pairing_id() == compare.__pairing.get_pairing_id()
                and self.__rate == compare.__rate
                and self.__amount == compare.__amount
                and self.__trade_date == compare.__trade_date
                and self.__order_id == compare.__order_id
                and self.__trade_type == compare.__trade_type)

    def __repr__(self: Trade) -> str:
        return """<model.trade.Trade
    trade_id=%r,
    pairing=%r,
    rate=%r,
    amount=%r,
    trade_date=%r,
    order_id=%r,
    trade_type=%r
>""" % (
            self.__trade_id,
            self.__pairing,
            self.__rate,
            self.__amount,
            self.__trade_date,
            self.__order_id,
            self.__trade_type
        )
