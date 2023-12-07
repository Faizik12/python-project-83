from __future__ import annotations

import logging
from typing import Any, TYPE_CHECKING

from psycopg2.extras import RealDictRow

from page_analyzer.url_db import db_operations

if TYPE_CHECKING:
    from psycopg2.extensions import connection

URLS_TABLE = 'urls'
URL_CHECKS_TABLE = 'url_checks'

CREATION_MESSAGE = 'The {entity} information has been added to the database'
RECEIPT_MESSAGE = 'The {entity} information was obtained from the database'
INSERT_ERROR = 'Error when trying to insert data'
SELECT_ERROR = 'Error when trying to select data'


def create_url(connection: connection, url: str) -> int | None:
    """Create a record URL in db, return record id or None if error occurs."""
    insertion_fields = ['name']
    insertion_data = {'name': url}
    returning_fields = ['id']

    returning = db_operations.insert_data(connection=connection,
                                          table=URLS_TABLE,
                                          fields=insertion_fields,
                                          data=insertion_data,
                                          returning=returning_fields)
    if returning is None:
        logging.error(INSERT_ERROR)
        return None

    url_id: int = returning[0]['id']
    logging.info(CREATION_MESSAGE.format(entity='URL'))
    return url_id


def create_check(connection: connection,
                 url_id: int,
                 data: dict[str, Any],
                 ) -> bool | None:
    """Create a record URL check in db, return True or None if error occurs."""
    data = data.copy()
    data.update(url_id=url_id)
    fields = ['url_id', 'status_code', 'h1',
              'title', 'description']

    returning = db_operations.insert_data(connection=connection,
                                          table=URL_CHECKS_TABLE,
                                          fields=fields,
                                          data=data)
    if returning is None:
        logging.error(INSERT_ERROR)
        return None

    logging.info(CREATION_MESSAGE.format(entity='URL check'))
    return True


def check_url(connection: connection, url: str) -> int | None:
    """Check for a URLs, return id, 0 if no record or None if error occurs."""
    fields: list[tuple[str, str]] = [('urls', 'id')]
    condition = (('urls', 'name'), url)

    urls = db_operations.select_data(connection=connection,
                                     table=URLS_TABLE,
                                     fields=fields,
                                     filtering=condition)
    if urls is None:
        logging.error(SELECT_ERROR)
        return None

    if not urls:
        url_id = 0
        logging.info(RECEIPT_MESSAGE.format(entity='URL'))
        return url_id
    # Будем возвращать везде где нечего вернуть None так как он освободиться,
    # ошибки будут обрабатываться через возбуждение

    url_id = urls[0]['id']
    logging.info(RECEIPT_MESSAGE.format(entity='URL'))
    return url_id


def get_urls(connection: connection) -> list[RealDictRow] | None:
    """Return a list of URL records or None if error occurs."""
    urls_fields = [('urls', 'id'), ('urls', 'name')]
    urls_sorting: list[tuple[tuple[str, str], str]]
    urls_sorting = [(('urls', 'created_at'), 'DESC')]

    checks_fields = [('url_checks', 'created_at'),
                     ('url_checks', 'status_code'),
                     ('url_checks', 'url_id')]
    checks_distinct = ('url_checks', 'url_id')
    checks_sorting = [(('url_checks', 'url_id'), 'ASC'),
                      (('url_checks', 'created_at'), 'DESC')]

    urls = db_operations.select_data(connection=connection,
                                     table=URLS_TABLE,
                                     fields=urls_fields,
                                     sorting=urls_sorting)
    if urls is None:
        logging.error(SELECT_ERROR)
        return None

    url_checks = db_operations.select_data(connection=connection,
                                           table=URL_CHECKS_TABLE,
                                           fields=checks_fields,
                                           distinct=checks_distinct,
                                           sorting=checks_sorting)
    if url_checks is None:
        logging.error(SELECT_ERROR)
        return None

    merged_data = _merge_urls_checks(urls, url_checks)

    logging.info(RECEIPT_MESSAGE.format(entity='URLs'))
    return merged_data


def get_url_checks(connection: connection,
                   url_id: int,
                   ) -> list[RealDictRow] | None:
    """Returns a list of URL checks, or None if an error occurred."""
    fields_for_checks = [('url_checks', 'id'),
                         ('url_checks', 'status_code'),
                         ('url_checks', 'h1'),
                         ('url_checks', 'title'),
                         ('url_checks', 'description'),
                         ('url_checks', 'created_at')]
    condition_for_checks = (('url_checks', 'url_id'), url_id)
    sorting_for_checks: list[tuple[tuple[str, str], str]]
    sorting_for_checks = [(('url_checks', 'created_at'), 'DESC')]

    url_checks = db_operations.select_data(connection=connection,
                                           table=URL_CHECKS_TABLE,
                                           fields=fields_for_checks,
                                           filtering=condition_for_checks,
                                           sorting=sorting_for_checks)
    if url_checks is None:
        logging.error(SELECT_ERROR)
        return None

    logging.info(RECEIPT_MESSAGE.format(entity='URLs checks and URL'))
    return url_checks


def get_url(connection: connection, url_id: int) -> RealDictRow | None:
    """Returns the URL record or None if an error occurred."""
    fields_for_url = [('urls', 'id'),
                      ('urls', 'name'),
                      ('urls', 'created_at')]
    condition_for_url = (('urls', 'id'), url_id)

    urls = db_operations.select_data(connection=connection,
                                     table=URLS_TABLE,
                                     fields=fields_for_url,
                                     filtering=condition_for_url)
    if urls is None:
        logging.error(SELECT_ERROR)
        return None

    if not urls:
        logging.info(RECEIPT_MESSAGE.format(entity='URL'))
        return RealDictRow()

    return urls[0]


def _merge_urls_checks(urls: list[RealDictRow],
                       url_checks: list[RealDictRow],
                       ) -> list[RealDictRow]:
    result = []
    for url in urls:
        url_id = url['id']
        order = RealDictRow(id=url_id,
                            name=url['name'])
        for check in url_checks:
            if check['url_id'] == url_id:
                order.update(created_at=check['created_at'],
                             status_code=check['status_code'])
                break
        result.append(order)
    return result
