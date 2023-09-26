import logging
from urllib.parse import urlparse

from page_analyzer.db import (
    commit_changes,
    insert_data,
    open_connection,
    select_data,
)
from validators import url as url_validator

URLS_TABLE = 'urls'
URL_DATA_TABLE = ''

URL_CREATION_MESSAGE = 'The URL information has been added to the database'
URL_RECEIVING_MESSAGE = 'The URL information was obtained from the database'
CONNECTION_ERROR = 'Error when trying to connect to the database'
INSERT_ERROR = 'Error when trying to insert data'
SELECT_ERROR = 'Error when trying to select data'


def normalize_url(source_url: str) -> str:
    url = urlparse(source_url)
    return f'{url.scheme}://{url.hostname}'


def validate_url(url: str) -> str:
    error = ''
    if not url:
        error = 'URL обязателен'
    elif len(url) > 255:
        error = 'URL превышает 255 символов'
    elif not url_validator(url):
        error = 'Некорректный URL'
    return error


def create_url(url: str) -> int | None:
    fields_insert = ['name']
    insertion_data = {'name': url}

    connection = open_connection()
    if connection is None:
        logging.error(CONNECTION_ERROR)
        return None

    insert_status = insert_data(connection=connection,
                                table=URLS_TABLE,
                                fields=fields_insert,
                                data=insertion_data)
    if not insert_status:
        logging.error(INSERT_ERROR)
        return None

    fields_select = ['id']
    condition = ('name', url)
    selection_data = select_data(connection=connection,
                                 table=URLS_TABLE,
                                 fields=fields_select,
                                 filter_=condition)
    if selection_data is None:
        logging.error(SELECT_ERROR)
        return None

    url_id: int = selection_data[0]['id']
    commit_changes(connection)
    logging.info(URL_CREATION_MESSAGE)
    return url_id


def check_url(url: str) -> int | None:
    fields = ['id']
    condition = ('name', url)

    connection = open_connection()
    if connection is None:
        logging.error(CONNECTION_ERROR)
        return None

    data = select_data(connection=connection,
                       table=URLS_TABLE,
                       fields=fields,
                       filter_=condition)
    if data is None:
        logging.error(SELECT_ERROR)
        return None

    if not data:
        url_id = 0
        commit_changes(connection)
        logging.info(URL_RECEIVING_MESSAGE)
        return url_id

    url_id = data[0]['id']

    commit_changes(connection)
    logging.info(URL_RECEIVING_MESSAGE)
    return url_id


def get_list_urls() -> list | None:
    fields = ['id', 'name']
    sorting = ('created_at', 'DESC')

    connection = open_connection()
    if connection is None:
        logging.error(CONNECTION_ERROR)
        return None

    data = select_data(connection=connection,
                       table=URLS_TABLE,
                       fields=fields,
                       sorting=sorting)
    if data is None:
        logging.error(SELECT_ERROR)
        return None

    commit_changes(connection)
    logging.info(URL_RECEIVING_MESSAGE)
    return data


def get_specific_url_info(url_id: str) -> dict | None:
    fields = ['id', 'name', 'created_at']
    condition = ('id', url_id)

    connection = open_connection()
    if connection is None:
        logging.error(CONNECTION_ERROR)
        return None

    selection_data = select_data(connection=connection,
                                 table=URLS_TABLE,
                                 fields=fields,
                                 filter_=condition)
    if selection_data is None:
        logging.error(SELECT_ERROR)
        return None

    data = iter(selection_data)
    result: dict = next(data, {})
    if result:
        result['created_at'] = result['created_at'].date()

    commit_changes(connection)
    logging.info(URL_RECEIVING_MESSAGE)
    return result
