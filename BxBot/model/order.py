from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from time import time

import pytz

from model.transaction import Transaction
from system.database import conn, cursor


class OrderType(Enum):
    BUY = 0
    SELL = 1


class Order(object):
    __order_id: int = None
    __transaction: Transaction = None
    __order_type: OrderType = None
    __amount: Decimal = None
    __rate: Decimal = None
    __pending: bool = None
    __date_open: datetime = None
    __date_close: datetime = None
    __cancelled: bool = None

    def __init__(
            self,
            order_id: int = None,
            transaction: Transaction = None,
            order_type: OrderType = None,
            amount: Decimal = None,
            rate: Decimal = None,
            pending: bool = None,
            date_open: datetime = None,
            date_close: datetime = None,
            cancelled: bool = None
    ):
        self.set_order_id(order_id) \
            .set_transaction(transaction) \
            .set_order_type(order_type) \
            .set_amount(amount) \
            .set_rate(rate) \
            .set_pending(pending) \
            .set_date_open(date_open)
        if date_close is not None:
            self.set_date_close(date_close)
        if cancelled is not None:
            self.set_cancelled(cancelled)

    def set_order_id(self: Order, order_id: int) -> Order:
        if order_id is not None:
            if order_id > 0:
                self.__order_id = order_id
            else:
                raise ValueError('order_id must be greater than 0.')
        else:
            raise ValueError('order_id must be not None.')
        return self

    def set_transaction(self: Order, transaction: Transaction) -> Order:
        if transaction is not None:
            if transaction.get_transaction_id() is not None:
                self.__transaction = transaction
            else:
                raise ValueError('transaction.transaction_id must be not None.')
        else:
            raise ValueError('transaction must be not None.')
        return self

    def get_order_type(self: Order) -> OrderType:
        return self.__order_type

    def set_order_type(self: Order, order_type: OrderType) -> Order:
        if order_type is not None:
            self.__order_type = order_type
        else:
            raise ValueError('order_type must be not None.')
        return self

    def get_amount(self: Order) -> Decimal:
        return self.__amount

    def set_amount(self: Order, amount: Decimal) -> Order:
        if amount is not None:
            self.__amount = amount
        else:
            raise ValueError('amount must be not None.')
        return self

    def get_rate(self: Order) -> Decimal:
        return self.__rate

    def set_rate(self: Order, rate: Decimal) -> Order:
        if rate is not None:
            self.__rate = rate
        else:
            raise ValueError('rate must be not None.')
        return self

    def get_pending(self: Order) -> bool:
        return self.__pending

    def set_pending(self: Order, pending: bool) -> Order:
        if pending is not None:
            self.__pending = pending
        else:
            raise ValueError('pending must be not None.')
        return self

    def set_date_open(self: Order, date_open: datetime) -> Order:
        if date_open is not None:
            self.__date_open = date_open
        else:
            raise ValueError('date_open must be not None.')
        return self

    def get_date_closed(self: Order) -> datetime:
        return self.__date_close

    def set_date_close(self: Order, date_close: datetime) -> Order:
        if date_close is not None:
            self.__date_close = date_close
        else:
            raise ValueError('date_close must be not None.')
        return self

    def get_cancelled(self: Order) -> bool:
        return self.__cancelled

    def set_cancelled(self: Order, cancelled: bool) -> Order:
        if cancelled is not None:
            self.__cancelled = cancelled
        else:
            raise ValueError('cancelled must be not None.')
        return self

    def persist_in_database(self: Order):
        query_select = "SELECT `order_id` FROM `order` WHERE `order_id` = %s"
        cursor.execute(query_select, (self.__order_id,))
        result = cursor.fetchone()
        if result is None:
            query_insert = """
            INSERT INTO `order`(
                `order_id`,
                `transaction_id`,
                `order_type`,
                `amount`,
                `rate`,
                `pending`,
                `date_open`,
                `date_close`,
                `cancelled`
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query_insert, (
                self.__order_id,
                self.__transaction.get_transaction_id(),
                self.__order_type.value,
                format(self.__amount, '.8f'),
                format(self.__rate, '.8f'),
                1 if self.__pending else 0,
                datetime.timestamp(self.__date_open),
                None if self.__date_close is None else datetime.timestamp(self.__date_close),
                None if self.__cancelled is None else 1 if self.__cancelled else 0
            ))
            conn.commit()
        else:
            query_update = """
            UPDATE `order`
               SET `order_id` = %s,
                   `transaction_id` = %s,
                   `order_type` = %s,
                   `amount` = %s,
                   `rate` = %s,
                   `pending` = %s,
                   `date_open` = %s,
                   `date_close` = %s,
                   `cancelled` = %s
             WHERE `order_id` = %s
            """
            cursor.execute(query_update, (
                self.__order_id,
                self.__transaction.get_transaction_id(),
                self.__order_type.value,
                format(self.__amount, '.8f'),
                format(self.__rate, '.8f'),
                1 if self.__pending else 0,
                datetime.timestamp(self.__date_open),
                None if self.__date_close is None else datetime.timestamp(self.__date_close),
                None if self.__cancelled is None else 1 if self.__cancelled else 0,
                self.__order_id
            ))
            conn.commit()


def close_orders_for_transaction_with_ids_not_in(transaction: Transaction, order_ids: str):
    if order_ids != "":
        query_update = """
                UPDATE `order`
                SET `pending`=%d,
                    `date_close`=%d,
                    `cancelled`=%d
                WHERE `transaction_id` = %d AND `order_id` NOT IN (%s)  AND `pending` = %d
            """ % (0, int(time()), 0, transaction.get_transaction_id(), order_ids, 1)
        cursor.execute(query_update)
        conn.commit()
    else:
        query_update = """
            UPDATE `order`
            SET `pending`=%d,
                `date_close`=%d,
                `cancelled`=%d
            WHERE `transaction_id` = %d AND `pending` = %d
        """ % (0, int(time()), 0, transaction.get_transaction_id(), 1)
        cursor.execute(query_update)
        conn.commit()


def get_orders_for_transaction(transaction: Transaction) -> list:
    result = []
    if transaction.get_transaction_id() is not None:
        query_select = """
            SELECT `order_id`,
                   `transaction_id`,
                   `order_type`,
                   `amount`,
                   `rate`,
                   `pending`,
                   `date_open`,
                   `date_close`,
                   `cancelled`
            FROM `order`
            WHERE `transaction_id` = %s
        """
        cursor.execute(query_select, (transaction.get_transaction_id(),))
        sql_orders = cursor.fetchall()
        for sql_order in sql_orders:
            order = Order(
                sql_order[0],
                transaction,
                OrderType.BUY if sql_order[2] == 0 else OrderType.SELL,
                sql_order[3],
                sql_order[4],
                sql_order[5] == 1,
                datetime.fromtimestamp(sql_order[6], pytz.timezone('Asia/Bangkok')),
                None if sql_order[7] is None else datetime.fromtimestamp(sql_order[7], pytz.timezone('Asia/Bangkok')),
                None if sql_order[8] is None else sql_order[8] == 1
            )
            result.append(order)
    return result
