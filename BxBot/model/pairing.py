from __future__ import annotations

import json
from decimal import Decimal
from typing import Optional

from model.currency import Currency
from system.database import cursor, conn


class Pairing(object):
    __pairing_id: int = None
    __primary_currency: Currency = None
    __secondary_currency: Currency = None
    __primary_min: Decimal = None
    __secondary_min: Decimal = None
    __active: bool = None
    __expected_percent_win: Decimal = None
    __step_number: int = None
    __step_factor: Decimal = None
    __step_gap_percent: Decimal = None

    def __init__(
            self,
            pairing_id: int = None,
            primary_currency: Currency = None,
            secondary_currency: Currency = None,
            primary_min: Decimal = None,
            secondary_min: Decimal = None,
            active: bool = None,
            expected_percent_win: Decimal = None,
            step_number: int = None,
            step_factor: Decimal = None,
            step_gap_percent: Decimal = None
    ):
        if (primary_currency is not None
                or secondary_currency is not None
                or primary_min is not None
                or secondary_min is not None
                or active is not None
                or expected_percent_win is not None
                or step_number is not None
                or step_factor is not None):
            self.set_pairing_id(pairing_id) \
                .set_primary_currency(primary_currency) \
                .set_secondary_currency(secondary_currency) \
                .set_primary_min(primary_min) \
                .set_secondary_min(secondary_min) \
                .set_active(active) \
                .set_expected_percent_win(expected_percent_win) \
                .set_step_number(step_number) \
                .set_step_factor(step_factor) \
                .set_step_gap_percent(step_gap_percent)
        elif pairing_id is not None:
            self.retrieve_from_database(pairing_id)
        else:
            raise ValueError('All values are set to None.')

    def get_pairing_id(self: Pairing) -> int:
        return self.__pairing_id

    def set_pairing_id(self: Pairing, pairing_id: int) -> Pairing:
        if pairing_id is not None:
            if pairing_id > 0:
                self.__pairing_id = pairing_id
            else:
                raise ValueError('pairing_id must be greater than 0.')
        else:
            raise ValueError('pairing_id must be not None.')
        return self

    def get_primary_currency(self: Pairing) -> Currency:
        return self.__primary_currency

    def set_primary_currency(self: Pairing, primary_currency: Currency) -> Pairing:
        if primary_currency is not None:
            self.__primary_currency = primary_currency
        else:
            raise ValueError('primary currency must be not None.')
        return self

    def get_secondary_currency(self: Pairing) -> Currency:
        return self.__secondary_currency

    def set_secondary_currency(self: Pairing, secondary_currency: Currency) -> Pairing:
        if secondary_currency is not None:
            self.__secondary_currency = secondary_currency
        else:
            raise ValueError('primary currency must be not None.')
        return self

    def set_primary_min(self: Pairing, primary_min: Decimal) -> Pairing:
        if primary_min is not None:
            self.__primary_min = primary_min
        else:
            raise ValueError('primary_min must be not None.')
        return self

    def set_secondary_min(self: Pairing, secondary_min: Decimal) -> Pairing:
        if secondary_min is not None:
            self.__secondary_min = secondary_min
        else:
            raise ValueError('secondary_min must be not None.')
        return self

    def set_active(self: Pairing, active: bool) -> Pairing:
        if active is not None:
            self.__active = active
        else:
            raise ValueError('active must be not None.')
        return self

    def get_expected_percent_win(self: Pairing) -> Decimal:
        return self.__expected_percent_win

    def set_expected_percent_win(self: Pairing, expected_percent_win: Optional[Decimal]) -> Pairing:
        self.__expected_percent_win = expected_percent_win
        return self

    def set_step_number(self: Pairing, step_number: Optional[int]) -> Pairing:
        self.__step_number = step_number
        return self

    def get_step_factor(self: Pairing) -> Decimal:
        return self.__step_factor

    def set_step_factor(self: Pairing, step_factor: Optional[Decimal]) -> Pairing:
        self.__step_factor = step_factor
        return self

    def get_step_gap_percent(self: Pairing) -> Decimal:
        return self.__step_gap_percent

    def set_step_gap_percent(self: Pairing, step_gap_percent: Optional[Decimal]) -> Pairing:
        self.__step_gap_percent = step_gap_percent
        return self

    def get_total_unit_to_invest_in_per_transaction(self: Pairing):
        total = Decimal("0.00000000")
        for i in range(0, self.__step_number):
            total += self.__step_factor ** i
        return total

    def persist_in_database(self: Pairing):
        if (self.__pairing_id is not None
                and self.__primary_currency is not None
                and self.__secondary_currency is not None
                and self.__primary_min is not None
                and self.__secondary_min is not None
                and self.__active is not None
        ):
            query_select = "SELECT `pairing_id` FROM `pairing` WHERE `pairing_id` = %s"
            cursor.execute(query_select, (self.__pairing_id,))
            result = cursor.fetchone()
            if result is None:
                query_insert = """
                INSERT INTO `pairing` (
                    `pairing_id`,
                    `primary_currency_id`,
                    `secondary_currency_id`,
                    `primary_min`,
                    `secondary_min`,
                    `active`
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """

                cursor.execute(query_insert, (
                    self.__pairing_id,
                    self.__primary_currency.get_currency_id(),
                    self.__secondary_currency.get_currency_id(),
                    format(self.__primary_min, '.8f'),
                    format(self.__secondary_min, '.8f'),
                    1 if self.__active else 0
                ))
            else:
                query_update = """
                UPDATE `pairing` SET
                    `pairing_id` = %s,
                    `primary_currency_id` = %s,
                    `secondary_currency_id` = %s,
                    `primary_min` = %s,
                    `secondary_min` = %s,
                    `active` = %s
                WHERE
                    `pairing_id` = %s
                """

                cursor.execute(query_update, (
                    self.__pairing_id,
                    self.__primary_currency.get_currency_id(),
                    self.__secondary_currency.get_currency_id(),
                    format(self.__primary_min, '.8f'),
                    format(self.__secondary_min, '.8f'),
                    1 if self.__active else 0,
                    self.__pairing_id
                ))
            conn.commit()
        else:
            ValueError('All attributes must be not None. %r' % self)

    def retrieve_from_database(self: Pairing, pairing_id: int):
        query_select = """
        SELECT `pairing_id`,
               `primary_currency_id`,
               `primary_currency`.`currency_code` as `primary_currency_code`,
               `primary_currency`.`currency_name` as `primary_currency_name`,
               `secondary_currency_id`,
               `secondary_currency`.`currency_code` as `secondary_currency_code`,
               `secondary_currency`.`currency_name` as `secondary_currency_name`,
               `primary_min`,
               `secondary_min`,
               `active`,
               `expected_percent_win`,
               `step_number`,
               `step_factor`,
               `step_gap_percent`
        FROM `pairing`
        INNER JOIN `currency` `primary_currency` ON `pairing`.`primary_currency_id` = `primary_currency`.`currency_id`
        INNER JOIN `currency` `secondary_currency` ON `pairing`.`secondary_currency_id` = `secondary_currency`.`currency_id`
        WHERE `pairing`.`pairing_id` = %s"""
        cursor.execute(query_select, (pairing_id,))
        result = cursor.fetchone()
        if result is not None:
            self.set_pairing_id(result[0]) \
                .set_primary_currency(Currency(result[1], result[2], result[3])) \
                .set_secondary_currency(Currency(result[4], result[5], result[6])) \
                .set_primary_min(result[7]) \
                .set_secondary_min(result[8]) \
                .set_active(result[9] == 1) \
                .set_expected_percent_win(result[10]) \
                .set_step_number(result[11]) \
                .set_step_factor(result[12]) \
                .set_step_gap_percent(result[13])
        else:
            raise ValueError("No pairing found with pairing_id=%d" % pairing_id)

    def __repr__(self: Pairing) -> str:
        return """<model.pairing.Pairing
    pairing_id=%r,
    primary_currency=%r,
    secondary_currency=%r,
    primary_min=%r,
    secondary_min=%r,
    active=%r
>""" % (
            self.__pairing_id,
            self.__primary_currency,
            self.__secondary_currency,
            self.__primary_min,
            self.__secondary_min,
            self.__active
        )

    @staticmethod
    def persist_trading_from_json(json_content: str):
        json_pairings = json.loads(json_content)
        for key in json_pairings:
            current_pairing_json = json_pairings.get(key)
            current_pairing = Pairing(
                int(current_pairing_json.get('pairing_id')),
                Currency(currency_code=current_pairing_json.get('primary_currency')),
                Currency(currency_code=current_pairing_json.get('secondary_currency')),
                Decimal(current_pairing_json.get('primary_min')),
                Decimal(current_pairing_json.get('secondary_min')),
                current_pairing_json.get('active')
            )
            current_pairing.persist_in_database()

    @staticmethod
    def get_trading_pairing() -> list:
        query_select = """
        SELECT `pairing_id`,
               `primary_currency_id`,
               `primary_currency`.`currency_code` as `primary_currency_code`,
               `primary_currency`.`currency_name` as `primary_currency_name`,
               `secondary_currency_id`,
               `secondary_currency`.`currency_code` as `secondary_currency_code`,
               `secondary_currency`.`currency_name` as `secondary_currency_name`,
               `primary_min`,
               `secondary_min`,
               `active`,
               `expected_percent_win`,
               `step_number`,
               `step_factor`,
               `step_gap_percent`
        FROM `pairing`
        INNER JOIN `currency` `primary_currency` ON `pairing`.`primary_currency_id` = `primary_currency`.`currency_id`
        INNER JOIN `currency` `secondary_currency` ON `pairing`.`secondary_currency_id` = `secondary_currency`.`currency_id`
        WHERE `pairing`.`trading` = %s"""
        cursor.execute(query_select, (1,))
        results = cursor.fetchall()
        return_value = []
        for result in results:
            pairing = Pairing(
                result[0],
                Currency(result[1], result[2], result[3]),
                Currency(result[4], result[5], result[6]),
                result[7],
                result[8],
                result[9] == 1,
                result[10],
                result[11],
                result[12],
                result[13]
            )
            return_value.append(pairing)
        return return_value
