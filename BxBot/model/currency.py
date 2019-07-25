from __future__ import annotations

from system.database import cursor


class Currency(object):
    __currency_id: int = None
    __currency_code: str = None
    __currency_name: str = None

    def __init__(
            self,
            currency_id: int = None,
            currency_code: str = None,
            currency_name: str = None
    ):
        if currency_id is not None or currency_name is not None:
            self.set_currency_id(currency_id) \
                .set_currency_code(currency_code) \
                .set_currency_name(currency_name)
        else:
            self.retrieve_from_database(currency_code)

    def get_currency_id(self: Currency) -> int:
        return self.__currency_id

    def set_currency_id(self: Currency, currency_id: int) -> Currency:
        if currency_id is not None:
            if currency_id > 0:
                self.__currency_id = currency_id
            else:
                raise ValueError('currency_id must be greater than 0.')
        else:
            raise ValueError('currency_id must be not None.')
        return self

    def get_currency_code(self: Currency) -> str:
        return self.__currency_code

    def set_currency_code(self: Currency, currency_code: str) -> Currency:
        if currency_code is not None:
            if len(currency_code) in (3, 4):
                self.__currency_code = currency_code
            else:
                raise ValueError('currency_code must be 3 characters long.')
        else:
            raise ValueError('currency_code must be not None.')
        return self

    def set_currency_name(self: Currency, currency_name: str) -> Currency:
        if currency_name is not None:
            if len(currency_name) > 0:
                self.__currency_name = currency_name
            else:
                raise ValueError('currency_name must be not empty.')
        else:
            raise ValueError('currency_name must be not None.')
        return self

    def retrieve_from_database(self: Currency, currency_code: str) -> Currency:
        if currency_code is not None:
            query = """
                SELECT `currency_id`, `currency_code`, `currency_name`
                FROM `currency`
                WHERE `currency_code` = %s
            """
            cursor.execute(query, (currency_code,))
            result = cursor.fetchone()
            if result is not None:
                self.set_currency_id(result[0])
                self.set_currency_code(result[1])
                self.set_currency_name(result[2])
            else:
                raise ValueError('Cannot find Currency with currency_code=%r', currency_code)
        else:
            raise ValueError('currency_code must be not None.')
        return self

    def __repr__(self: Currency) -> str:
        return "<model.currency.Currency currency_id=%r, currency_code=%r, currency_name=%r>" % (
        self.__currency_id, self.__currency_code, self.__currency_name)
