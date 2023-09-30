import logging
from typing import Any
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from page_analyzer.db import (
    commit_changes,
    insert_data,
    open_connection,
    select_data,
)
from psycopg2.extras import RealDictRow
from validators import url as url_validator

URLS_TABLE = 'urls'
URL_CHECKS_TABLE = 'url_checks'

URL_CREATION_MESSAGE = 'The URL information has been added to the database'
URL_RECEIVING_MESSAGE = 'The URL information was obtained from the database'
SITE_CONNECTION_MESSAGE = 'The response from the site was received'
DB_CONNECTION_ERROR = 'Error when trying to connect to the database'
INSERT_ERROR = 'Error when trying to insert data'
SELECT_ERROR = 'Error when trying to select data'


def normalize_url(source_url: str) -> str:
    url = urlparse(source_url)
    return f'{url.scheme}://{url.hostname}'


def validate_url(url: str) -> str:
    error: str = ''
    if not url:
        error = 'URL обязателен'
    elif len(url) > 255:
        error = 'URL превышает 255 символов'
    elif not url_validator(url):
        error = 'Некорректный URL'
    return error


def create_url(url: str) -> int | None:
    insertion_fields = ['name']
    insertion_data = {'name': url}

    connection = open_connection()
    if connection is None:
        logging.error(DB_CONNECTION_ERROR)
        return None

    insert_status = insert_data(connection=connection,
                                table=URLS_TABLE,
                                fields=insertion_fields,
                                data=insertion_data)
    if not insert_status:
        logging.error(INSERT_ERROR)
        return None

    selection_fields: list[tuple[str, str]] = [('urls', 'id')]
    condition = (('urls', 'name'), url)
    selection_data = select_data(connection=connection,
                                 table=URLS_TABLE,
                                 fields=selection_fields,
                                 filtering=condition)
    if selection_data is None:
        logging.error(SELECT_ERROR)
        return None

    url_id: int = selection_data[0]['id']
    commit_changes(connection)
    logging.info(URL_CREATION_MESSAGE)
    return url_id


def create_check(url_id: int, data: dict[str, Any],) -> bool | None:
    data.setdefault('status_code')
    data.setdefault('h1')
    data.setdefault('title')
    data.setdefault('description')
    data.update(url_id=url_id)
    fields = ['url_id', 'status_code', 'h1',
              'title', 'description']

    connection = open_connection()
    if connection is None:
        logging.error(DB_CONNECTION_ERROR)
        return None

    insert_status = insert_data(connection=connection,
                                table=URL_CHECKS_TABLE,
                                fields=fields,
                                data=data)
    if not insert_status:
        logging.error(INSERT_ERROR)
        return None

    commit_changes(connection)
    logging.info(URL_CREATION_MESSAGE)
    return True


def check_url(url: str) -> int | None:
    fields: list[tuple[str, str]] = [('urls', 'id')]
    condition = (('urls', 'name'), url)

    connection = open_connection()
    if connection is None:
        logging.error(DB_CONNECTION_ERROR)
        return None

    list_urls = select_data(connection=connection,
                            table=URLS_TABLE,
                            fields=fields,
                            filtering=condition)
    if list_urls is None:
        logging.error(SELECT_ERROR)
        return None

    if not list_urls:
        url_id = 0
        commit_changes(connection)
        logging.info(URL_RECEIVING_MESSAGE)
        return url_id

    url_id = list_urls[0]['id']

    commit_changes(connection)
    logging.info(URL_RECEIVING_MESSAGE)
    return url_id


def _normalize_created_at(data_list: list) -> None:
    for item in data_list:
        if item['created_at'] is not None:
            item['created_at'] = item['created_at'].date()


def get_list_urls() -> list[RealDictRow] | None:
    fields = [('urls', 'id'),
              ('urls', 'name'),
              ('url_checks', 'created_at'),
              ('url_checks', 'status_code')]
    distinct = ('urls', 'created_at')
    joining = (('url_checks', 'url_id'), 'id')
    sorting = [(('urls', 'created_at'), 'DESC'),
               (('url_checks', 'created_at'), 'DESC')]

    connection = open_connection()
    if connection is None:
        logging.error(DB_CONNECTION_ERROR)
        return None

    list_urls = select_data(connection=connection,
                            table=URLS_TABLE,
                            distinct=distinct,
                            fields=fields,
                            joining=joining,
                            sorting=sorting)  # type: ignore
    if list_urls is None:
        logging.error(SELECT_ERROR)
        return None

    if list_urls:
        _normalize_created_at(list_urls)

    commit_changes(connection)
    logging.info(URL_RECEIVING_MESSAGE)
    return list_urls


def get_specific_url_info(
        url_id: int) -> tuple[RealDictRow | dict, list[RealDictRow]] | None:
    fields_for_url = [('urls', 'id'),
                      ('urls', 'name'),
                      ('urls', 'created_at')]
    condition_for_url = (('urls', 'id'), url_id)

    connection = open_connection()
    if connection is None:
        logging.error(DB_CONNECTION_ERROR)
        return None

    list_urls = select_data(connection=connection,
                            table=URLS_TABLE,
                            fields=fields_for_url,
                            filtering=condition_for_url)
    if list_urls is None:
        logging.error(SELECT_ERROR)
        return None

    fields_for_checks = [('url_checks', 'id'),
                         ('url_checks', 'status_code'),
                         ('url_checks', 'h1'),
                         ('url_checks', 'title'),
                         ('url_checks', 'description'),
                         ('url_checks', 'created_at')]
    condition_for_checks = (('url_checks', 'url_id'), url_id)
    sorting = [(('url_checks', 'created_at'), 'DESC')]

    list_url_checks = select_data(connection=connection,
                                  table=URL_CHECKS_TABLE,
                                  fields=fields_for_checks,
                                  filtering=condition_for_checks,
                                  sorting=sorting)  # type: ignore
    if list_url_checks is None:
        logging.error(SELECT_ERROR)
        return None

    iter_list_urls = iter(list_urls)
    url_data: RealDictRow | dict = next(iter_list_urls, {})
    if url_data:
        url_data['created_at'] = url_data['created_at'].date()

    if list_url_checks:
        _normalize_created_at(list_url_checks)

    commit_changes(connection)
    logging.info(URL_RECEIVING_MESSAGE)
    return url_data, list_url_checks


def get_url_name(url_id: int) -> str | None:
    fields: list[tuple[str, str]] = [('urls', 'name')]
    condition = (('urls', 'id'), url_id)

    connection = open_connection()
    if connection is None:
        logging.error(DB_CONNECTION_ERROR)
        return None

    list_urls = select_data(connection=connection,
                            table=URLS_TABLE,
                            fields=fields,
                            filtering=condition)
    if list_urls is None:
        logging.error(SELECT_ERROR)
        return None

    if not list_urls:
        empty_url = ''
        commit_changes(connection)
        logging.info(URL_RECEIVING_MESSAGE)
        return empty_url

    url: str = list_urls[0]['name']

    commit_changes(connection)
    logging.info(URL_RECEIVING_MESSAGE)
    return url


def get_site_response(url: str) -> requests.Response | None:
    response = None

    try:
        response = requests.get(url)
    except requests.ConnectionError as error:
        logging.exception(error)
        return response

    logging.info(SITE_CONNECTION_MESSAGE)
    return response


def extract_necessary_data(response: requests.Response) -> dict[str, Any]:
    content = response.text
    status_code = response.status_code
    result: dict[str, Any] = {'status_code': status_code}

    html_tree = BeautifulSoup(content, 'html.parser')

    tag_h1 = html_tree.h1
    tag_title = html_tree.title
    tag_meta_desc = html_tree.find('meta', attrs={'name': 'description'})

    content_h1: str | None = None
    content_title: str | None = None
    content_desc: str | None = None

    if tag_h1 is not None:
        content_h1 = str(tag_h1.string)
    if tag_title is not None:
        content_title = str(tag_title.string)
    if tag_meta_desc is not None:
        content_desc = str(tag_meta_desc.get('content'))  # type: ignore

    matches_fields = ('h1', 'title', 'description')
    matches_value = (content_h1, content_title, content_desc)
    matches = {key: value if value is not None else None
               for key, value in zip(matches_fields, matches_value)}
    result.update(matches)

    return result
