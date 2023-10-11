from __future__ import annotations

import logging
from typing import Any, TYPE_CHECKING

from psycopg2.extras import RealDictRow

from page_analyzer.url_db_handler import db_operations

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

    insert_status = db_operations.insert_data(connection=connection,
                                              table=URLS_TABLE,
                                              fields=insertion_fields,
                                              data=insertion_data)
    if not insert_status:
        logging.error(INSERT_ERROR)
        return None

    selection_fields: list[tuple[str, str]] = [('urls', 'id')]
    condition = (('urls', 'name'), url)
    selection_data = db_operations.select_data(connection=connection,
                                               table=URLS_TABLE,
                                               fields=selection_fields,
                                               filtering=condition)
    if selection_data is None:
        logging.error(SELECT_ERROR)
        return None

    url_id: int = selection_data[0]['id']
    logging.info(CREATION_MESSAGE.format(entity='URL'))
    return url_id


def create_check(connection: connection,
                 url_id: int,
                 data: dict[str, Any],
                 ) -> bool | None:
    """Create a record URL check in db, return True or None if error occurs."""
    data.update(url_id=url_id)
    fields = ['url_id', 'status_code', 'h1',
              'title', 'description']

    insert_status = db_operations.insert_data(connection=connection,
                                              table=URL_CHECKS_TABLE,
                                              fields=fields,
                                              data=data)
    if not insert_status:
        logging.error(INSERT_ERROR)
        return None

    logging.info(CREATION_MESSAGE.format(entity='URL check'))
    return True


def check_url(connection: connection, url: str) -> int | None:
    """Check for a URLs, return id, 0 if no record or None if error occurs."""
    fields: list[tuple[str, str]] = [('urls', 'id')]
    condition = (('urls', 'name'), url)

    list_urls = db_operations.select_data(connection=connection,
                                          table=URLS_TABLE,
                                          fields=fields,
                                          filtering=condition)
    if list_urls is None:
        logging.error(SELECT_ERROR)
        return None

    if not list_urls:
        url_id = 0
        logging.info(RECEIPT_MESSAGE.format(entity='URL'))
        return url_id

    url_id = list_urls[0]['id']
    logging.info(RECEIPT_MESSAGE.format(entity='URL'))
    return url_id


def _normalize_created_at(record_list: list[RealDictRow]) -> None:
    """Change the created_at of records leaving only the date."""
    for record in record_list:
        record['created_at'] = record['created_at'].date()


def get_list_urls(connection: connection) -> list[RealDictRow] | None:
    """Return a list of URL records or None if error occurs."""
    fields = [('urls', 'id'),
              ('urls', 'name'),
              ('url_checks', 'created_at'),
              ('url_checks', 'status_code')]
    distinct = ('urls', 'created_at')
    joining = (('url_checks', 'url_id'), 'id')
    sorting = [(('urls', 'created_at'), 'DESC'),
               (('url_checks', 'created_at'), 'DESC')]

    list_urls = db_operations.select_data(connection=connection,
                                          table=URLS_TABLE,
                                          distinct=distinct,
                                          fields=fields,
                                          joining=joining,
                                          sorting=sorting)
    if list_urls is None:
        logging.error(SELECT_ERROR)
        return None

    _normalize_created_at(list_urls)
    logging.info(RECEIPT_MESSAGE.format(entity='URLs'))
    return list_urls


def get_specific_url_info(connection: connection,
                          url_id: int,
                          ) -> tuple[RealDictRow, list[RealDictRow]] | None:
    """Return URL record and a list of its checks, or None if error occurs."""
    fields_for_url = [('urls', 'id'),
                      ('urls', 'name'),
                      ('urls', 'created_at')]
    condition_for_url = (('urls', 'id'), url_id)

    list_urls = db_operations.select_data(connection=connection,
                                          table=URLS_TABLE,
                                          fields=fields_for_url,
                                          filtering=condition_for_url)
    if list_urls is None:
        logging.error(SELECT_ERROR)
        return None

    if not list_urls:
        logging.info(RECEIPT_MESSAGE.format(entity='URL'))
        return RealDictRow(), []

    fields_for_checks = [('url_checks', 'id'),
                         ('url_checks', 'status_code'),
                         ('url_checks', 'h1'),
                         ('url_checks', 'title'),
                         ('url_checks', 'description'),
                         ('url_checks', 'created_at')]
    condition_for_checks = (('url_checks', 'url_id'), url_id)
    sorting_for_checks: list[tuple[tuple[str, str], str]]
    sorting_for_checks = [(('url_checks', 'created_at'), 'DESC')]

    list_url_checks = db_operations.select_data(connection=connection,
                                                table=URL_CHECKS_TABLE,
                                                fields=fields_for_checks,
                                                filtering=condition_for_checks,
                                                sorting=sorting_for_checks)
    if list_url_checks is None:
        logging.error(SELECT_ERROR)
        return None

    _normalize_created_at(list_urls)
    url_data = list_urls[0]

    _normalize_created_at(list_url_checks)
    logging.info(RECEIPT_MESSAGE.format(entity='URLs checks and URL'))
    return url_data, list_url_checks


def get_url_name(connection: connection, url_id: int) -> str | None:
    """Return the name of the URL or None if error occurs."""
    fields: list[tuple[str, str]] = [('urls', 'name')]
    condition = (('urls', 'id'), url_id)

    list_urls = db_operations.select_data(connection=connection,
                                          table=URLS_TABLE,
                                          fields=fields,
                                          filtering=condition)
    if list_urls is None:
        logging.error(SELECT_ERROR)
        return None

    if not list_urls:
        empty_url = ''
        logging.info(RECEIPT_MESSAGE.format(entity='url name'))
        return empty_url

    url: str = list_urls[0]['name']
    logging.info(RECEIPT_MESSAGE.format(entity='url name'))
    return url
