from __future__ import annotations

from datetime import datetime
from typing import Optional

import pytz

from model.pairing import Pairing
from system.database import cursor, conn


class Transaction(object):
    __transaction_id: int = None
    __pairing: Pairing = None
    __performing: bool = None
    __start_date: datetime = None
    __end_date: datetime = None

    def __init__(
            self: Transaction,
            transaction_id: Optional[int],
            pairing: Pairing,
            performing: bool,
            start_date: datetime,
            end_date: Optional[datetime]
    ):
        if transaction_id is not None:
            self.set_trandaction_id(transaction_id)
        self.set_pairing(pairing) \
            .set_performing(performing) \
            .set_start_date(start_date)
        if end_date is not None:
            self.set_end_date(end_date)

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

    def persist_in_database(self: Transaction):
        if (self.__transaction_id is not None
                and self.__pairing is not None
                and self.__performing is not None
                and self.__start_date is not None):
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
                    `end_date`
                ) VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(query_insert, (
                    self.__transaction_id,
                    self.__pairing.get_pairing_id(),
                    1 if self.__performing else 0,
                    datetime.timestamp(self.__start_date),
                    datetime.timestamp(self.__end_date) if self.__end_date is not None else self.__end_date
                ))
                conn.commit()
            else:
                query_update = """
                UPDATE `transaction`
                SET `transaction_id` = %s,
                    `pairing_id` = %s,
                    `performing` = %s,
                    `start_date` = %s,
                    `end_date` = %s
                WHERE `transaction_id` = %s
                """
                cursor.execute(query_update, (
                    self.__transaction_id,
                    self.__pairing.get_pairing_id(),
                    1 if self.__performing else 0,
                    datetime.timestamp(self.__start_date),
                    datetime.timestamp(self.__end_date) if self.__end_date is not None else self.__end_date,
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
                                `end_date`
                            ) VALUES (%s, %s, %s, %s)
                            """
            cursor.execute(query_insert, (
                self.__pairing.get_pairing_id(),
                1 if self.__performing else 0,
                datetime.timestamp(self.__start_date),
                datetime.timestamp(self.__end_date) if self.__end_date is not None else self.__end_date
            ))
            conn.commit()
            self.set_trandaction_id(cursor.lastrowid)
        else:
            raise ValueError('All attributes must be not None. %r' % self)

    def execute(self):
        print("Executing transaction")


def get_performing_transaction_for_pairing(pairing: Pairing) -> Transaction:
    query_select = """
    SELECT `transaction_id`,
           `pairing_id`,
           `performing`,
           `start_date`,
           `end_date`
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
            datetime.fromtimestamp(result[4], pytz.timezone('Asia/Bangkok')) if result[4] is not None else result[4]
        )
    return transaction
