from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

import pytz

from model.pairing import Pairing
from system.database import conn, cursor


class Transaction(object):
    __transaction_id: int = None
    __pairing: Pairing = None
    __performing: bool = None
    __start_date: datetime = None
    __end_date: datetime = None
    __primary_start_balance: Decimal = None
    __secondary_start_balance: Decimal = None

    def __init__(
            self: Transaction,
            transaction_id: Optional[int],
            pairing: Pairing,
            performing: bool,
            start_date: datetime,
            end_date: Optional[datetime],
            primary_start_balance: Decimal,
            secondary_start_balance: Decimal
    ):
        if transaction_id is not None:
            self.set_trandaction_id(transaction_id)
        self.set_pairing(pairing) \
            .set_performing(performing) \
            .set_start_date(start_date) \
            .set_primary_start_balance(primary_start_balance) \
            .set_secondary_start_balance(secondary_start_balance)
        if end_date is not None:
            self.set_end_date(end_date)

    def get_transaction_id(self: Transaction) -> Optional[int]:
        return self.__transaction_id

    def set_trandaction_id(self: Transaction, transaction_id: int) -> Transaction:
        if transaction_id is not None:
            if transaction_id > 0:
                self.__transaction_id = transaction_id
            else:
                raise ValueError('transaction_id must be greater than 0.')
        else:
            raise ValueError('transaction_id must be not None.')
        return self

    def set_pairing(self: Transaction, pairing: Pairing) -> Transaction:
        if pairing is not None:
            self.__pairing = pairing
        else:
            raise ValueError('pairing must be not None.')
        return self

    def set_performing(self: Transaction, performing: bool) -> Transaction:
        if performing is not None:
            self.__performing = performing
        else:
            raise ValueError('performing must be not None.')
        return self

    def set_start_date(self: Transaction, start_date: datetime) -> Transaction:
        if start_date is not None:
            self.__start_date = start_date
        else:
            raise ValueError('start_date must be not None.')
        return self

    def set_end_date(self: Transaction, end_date: datetime) -> Transaction:
        if end_date is not None:
            self.__end_date = end_date
        else:
            raise ValueError('end_date must be not None.')
        return self

    def set_primary_start_balance(self: Transaction, primary_start_balance: Decimal) -> Transaction:
        if primary_start_balance is not None:
            self.__primary_start_balance = primary_start_balance
        else:
            raise ValueError('primary_start_balance must be not None.')
        return self

    def set_secondary_start_balance(self: Transaction, secondary_start_balance: Decimal) -> Transaction:
        if secondary_start_balance is not None:
            self.__secondary_start_balance = secondary_start_balance
        else:
            raise ValueError('secondary_start_balance must be not None')
        return self

    def persist_in_database(self: Transaction):
        if (self.__transaction_id is not None
                and self.__pairing is not None
                and self.__performing is not None
                and self.__start_date is not None
                and self.__primary_start_balance is not None
                and self.__secondary_start_balance is not None):
            query_select = """
            SELECT `transaction_id` FROM `transaction` WHERE `transaction_id` = %s
            """
            cursor.execute(query_select, (self.__transaction_id,))
            result = cursor.fetchone()
            if result is None:
                query_insert = """
                INSERT INTO `transaction`(
                    `transaction_id`,
                    `pairing_id`,
                    `performing`,
                    `start_date`,
                    `end_date`,
                    `primary_start_balance`,
                    `secondary_start_balance`
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(query_insert, (
                    self.__transaction_id,
                    self.__pairing.get_pairing_id(),
                    1 if self.__performing else 0,
                    datetime.timestamp(self.__start_date),
                    datetime.timestamp(self.__end_date) if self.__end_date is not None else self.__end_date,
                    format(self.__primary_start_balance, '.8f'),
                    format(self.__secondary_start_balance, '.8f')
                ))
                conn.commit()
            else:
                query_update = """
                UPDATE `transaction`
                SET `transaction_id` = %s,
                    `pairing_id` = %s,
                    `performing` = %s,
                    `start_date` = %s,
                    `end_date` = %s,
                    `primary_start_balance` = %s,
                    `secondary_start_balance` = %s
                WHERE `transaction_id` = %s
                """
                cursor.execute(query_update, (
                    self.__transaction_id,
                    self.__pairing.get_pairing_id(),
                    1 if self.__performing else 0,
                    datetime.timestamp(self.__start_date),
                    datetime.timestamp(self.__end_date) if self.__end_date is not None else self.__end_date,
                    format(self.__primary_start_balance, '.8f'),
                    format(self.__secondary_start_balance, '.8f'),
                    self.__transaction_id
                ))
                conn.commit()
        elif (self.__pairing is not None
              and self.__performing is not None
              and self.__start_date is not None):
            query_insert = """
                            INSERT INTO `transaction`(
                                `pairing_id`,
                                `performing`,
                                `start_date`,
                                `end_date`,
                                `primary_start_balance`,
                                `secondary_start_balance`
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                            """
            cursor.execute(query_insert, (
                self.__pairing.get_pairing_id(),
                1 if self.__performing else 0,
                datetime.timestamp(self.__start_date),
                datetime.timestamp(self.__end_date) if self.__end_date is not None else self.__end_date,
                format(self.__primary_start_balance, '.8f'),
                format(self.__secondary_start_balance, '.8f')
            ))
            conn.commit()
            self.set_trandaction_id(cursor.lastrowid)
        else:
            raise ValueError('All attributes must be not None. %r' % self)

    def __repr__(self: Transaction):
        return """<model.transaction.Transaction
    transaction_id=%r,
    pairing=%s,
    performing=%r,
    start_date=%r,
    end_date=%r,
    primary_start_balance=%r,
    secondary_start_balance=%r
>
        """ % (
            self.__transaction_id,
            "None" if self.__pairing is None else self.__pairing.__repr__().replace("\n", "\n\t"),
            self.__performing,
            self.__start_date,
            self.__end_date,
            self.__primary_start_balance,
            self.__secondary_start_balance
        )


def get_performing_transaction_for_pairing(pairing: Pairing) -> Transaction:
    query_select = """
    SELECT `transaction_id`,
           `pairing_id`,
           `performing`,
           `start_date`,
           `end_date`,
           `primary_start_balance`,
           `secondary_start_balance`
    FROM `transaction`
    WHERE `pairing_id` = %s and `performing` = %s
    """

    cursor.execute(query_select, (pairing.get_pairing_id(), 1))
    result = cursor.fetchone()
    transaction = None
    if result is not None:
        transaction = Transaction(
            result[0],
            pairing,
            result[2],
            datetime.fromtimestamp(result[3], pytz.timezone('Asia/Bangkok')),
            datetime.fromtimestamp(result[4], pytz.timezone('Asia/Bangkok')) if result[4] is not None else result[4],
            result[5],
            result[6]
        )
    return transaction
