from __future__ import annotations

import logging
import typing as t

import psycopg2

from page_analyzer.url_db import db_operations

if t.TYPE_CHECKING:
    from datetime import datetime

    from psycopg2.extensions import connection

URLS_TABLE = 'urls'
URL_CHECKS_TABLE = 'url_checks'

CREATION_MESSAGE = 'The {entity} information has been added to the database'
RECEIPT_MESSAGE = 'The {entity} information was obtained from the database'
LOWER_LEVEL_ERROR = 'Error at the lower level'


def create_url(connection: connection, url: str) -> int:
    """Create a record URL in db, return record id."""
    try:
        returning = db_operations.insert_data(connection=connection,
                                              table=URLS_TABLE,
                                              fields=['name'],
                                              data={'name': url},
                                              returning=['id'])
    except psycopg2.Error:
        logging.error(LOWER_LEVEL_ERROR)
        raise

    url_id: int = returning[0].id  # type: ignore
    logging.info(CREATION_MESSAGE.format(entity='URL'))
    return url_id


def create_check(connection: connection,
                 url_id: int,
                 data: dict[str, t.Any],
                 ) -> None:
    """Create a record URL check in db, return None."""
    try:
        db_operations.insert_data(connection=connection,
                                  table=URL_CHECKS_TABLE,
                                  fields=['url_id',
                                          'status_code',
                                          'h1',
                                          'title',
                                          'description'],
                                  data=data | {'url_id': url_id})
    except psycopg2.Error:
        logging.error(LOWER_LEVEL_ERROR)
        raise

    logging.info(CREATION_MESSAGE.format(entity='URL check'))


def check_url(connection: connection, url: str) -> int | None:
    """Check for a URLs, return id or None if no record."""
    try:
        urls = db_operations.select_data(connection=connection,
                                         table=URLS_TABLE,
                                         fields=[('urls', 'id')],
                                         filtering=(('urls', 'name'), url))
    except psycopg2.Error:
        logging.error(LOWER_LEVEL_ERROR)
        raise

    if not urls:
        logging.info(RECEIPT_MESSAGE.format(entity='URL'))
        return None

    url_id: int = urls[0].id  # type: ignore
    logging.info(RECEIPT_MESSAGE.format(entity='URL'))
    return url_id


def get_urls(connection: connection) -> t.Sequence[t.NamedTuple]:
    """Return a list of URL records."""
    urls_fields = [('urls', 'id'), ('urls', 'name')]
    urls_sorting: list[tuple[tuple[str, str], str]]
    urls_sorting = [(('urls', 'created_at'), 'DESC')]

    checks_fields = [('url_checks', 'created_at'),
                     ('url_checks', 'status_code'),
                     ('url_checks', 'url_id')]
    checks_distinct = ('url_checks', 'url_id')
    checks_sorting = [(('url_checks', 'url_id'), 'ASC'),
                      (('url_checks', 'created_at'), 'DESC')]

    try:
        urls = db_operations.select_data(connection=connection,
                                         table=URLS_TABLE,
                                         fields=urls_fields,
                                         sorting=urls_sorting)
        url_checks = db_operations.select_data(connection=connection,
                                               table=URL_CHECKS_TABLE,
                                               fields=checks_fields,
                                               distinct=checks_distinct,
                                               sorting=checks_sorting)
    except psycopg2.Error:
        logging.error(LOWER_LEVEL_ERROR)
        raise

    merged_data = _merge_urls_checks(urls, url_checks)
    logging.info(RECEIPT_MESSAGE.format(entity='URLs'))
    return merged_data


def get_url_checks(connection: connection,
                   url_id: int,
                   ) -> list[t.NamedTuple]:
    """Returns a list of URL checks."""
    fields = [('url_checks', 'id'),
              ('url_checks', 'status_code'),
              ('url_checks', 'h1'),
              ('url_checks', 'title'),
              ('url_checks', 'description'),
              ('url_checks', 'created_at')]
    condition = (('url_checks', 'url_id'), url_id)
    sorting: list[tuple[tuple[str, str], str]]
    sorting = [(('url_checks', 'created_at'), 'DESC')]

    try:
        url_checks = db_operations.select_data(connection=connection,
                                               table=URL_CHECKS_TABLE,
                                               fields=fields,
                                               filtering=condition,
                                               sorting=sorting)
    except psycopg2.Error:
        logging.error(LOWER_LEVEL_ERROR)
        raise

    logging.info(RECEIPT_MESSAGE.format(entity='URLs checks and URL'))
    return url_checks


def get_url(connection: connection, url_id: int) -> t.NamedTuple | None:
    """Returns the URL record or None if no record."""
    try:
        urls = db_operations.select_data(connection=connection,
                                         table=URLS_TABLE,
                                         fields=[('urls', 'id'),
                                                 ('urls', 'name'),
                                                 ('urls', 'created_at')],
                                         filtering=(('urls', 'id'), url_id))
    except psycopg2.Error:
        logging.error(LOWER_LEVEL_ERROR)
        raise

    if not urls:
        logging.info(RECEIPT_MESSAGE.format(entity='URL'))
        return None

    return urls[0]


def _merge_urls_checks(urls: t.Sequence[t.NamedTuple],
                       url_checks: t.Sequence[t.NamedTuple],
                       ) -> t.Sequence[t.NamedTuple]:
    '''Return the merged list of URLs and their checks.'''
    class Record(t.NamedTuple):
        id: int
        name: str
        created_at: datetime | None = None
        status_code: int | None = None

    result = []
    for url in urls:
        for check in url_checks:
            if check.url_id == url.id:  # type: ignore
                record = Record(url.id,  # type: ignore
                                url.name,  # type: ignore
                                check.created_at,  # type: ignore
                                check.status_code)  # type: ignore
                break
        else:
            record = Record(url.id, url.name)  # type: ignore
        result.append(record)
    return result
