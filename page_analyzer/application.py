from __future__ import annotations

import logging
import os
from typing import NoReturn, TYPE_CHECKING

import dotenv
import flask

from page_analyzer import site_processing
from page_analyzer import url_db_handler
from page_analyzer import url_processing

if TYPE_CHECKING:
    from werkzeug.exceptions import HTTPException
    from werkzeug.wrappers import Response

dotenv.load_dotenv()

app = flask.Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY')

WARNING_MESSAGE_TYPE = 'danger'
SUCCES_MESSAGE_TYPE = 'success'
INFO_MESSAGE_TYPE = 'info'

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] [%(levelname)s] '
                           '[%(module)s.%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


@app.get('/')
def index() -> str:
    """Return the main page."""
    messages = flask.get_flashed_messages(with_categories=True)
    return flask.render_template('index.html', messages=messages)


@app.post('/urls')
def post_urls() -> Response | tuple[str, int] | NoReturn:
    """Process a request to create a url record."""
    url = flask.request.form.get('url', '')
    error = url_processing.validate_url(url)

    if error:
        flask.flash(error, WARNING_MESSAGE_TYPE)
        messages = flask.get_flashed_messages(with_categories=True)
        return flask.render_template('index.html',
                                     messages=messages,
                                     url=url), 422

    normalized_url = url_processing.normalize_url(url)

    connection = url_db_handler.open_connection()
    if connection is None:
        flask.abort(500)

    url_id = url_db_handler.check_url(connection, normalized_url)
    if url_id is None:
        url_db_handler.close_connection(connection)
        flask.abort(500)

    if url_id:
        url_db_handler.close_connection(connection)
        flask.flash('Страница уже существует', INFO_MESSAGE_TYPE)
        return flask.redirect(flask.url_for('get_url', id=url_id))

    url_id = url_db_handler.create_url(connection, normalized_url)
    if url_id is None:
        url_db_handler.close_connection(connection)
        flask.abort(500)

    url_db_handler.close_connection(connection)
    flask.flash('Страница успешно добавлена', SUCCES_MESSAGE_TYPE)
    return flask.redirect(flask.url_for('get_url', id=url_id))


@app.get('/urls')
def get_urls() -> str | NoReturn:
    """Return the page with the list of URLs."""
    connection = url_db_handler.open_connection()
    if connection is None:
        flask.abort(500)

    list_urls = url_db_handler.get_list_urls(connection=connection)

    if list_urls is None:
        url_db_handler.close_connection(connection=connection)
        flask.abort(500)

    url_db_handler.close_connection(connection=connection)
    messages = flask.get_flashed_messages(with_categories=True)
    return flask.render_template('urls.html', messages=messages, urls=list_urls)


@app.get('/urls/<int:id>')
def get_url(id: int) -> str | NoReturn:
    """Return the page to a specific URL."""
    connection = url_db_handler.open_connection()
    if connection is None:
        flask.abort(500)

    data = url_db_handler.get_specific_url_info(connection, id)
    if data is None:
        url_db_handler.close_connection(connection=connection)
        flask.abort(500)

    url_data, checks_list = data
    if not url_data:
        url_db_handler.close_connection(connection=connection)
        flask.abort(404)

    url_db_handler.close_connection(connection=connection)
    messages = flask.get_flashed_messages(with_categories=True)
    return flask.render_template('url.html',
                                 messages=messages,
                                 url=url_data,
                                 checks=checks_list)


@app.post('/urls/<int:id>/checks')
def post_checks(id: int) -> Response | NoReturn:
    """Process a request to create a URL verification record."""
    connection = url_db_handler.open_connection()
    if connection is None:
        flask.abort(500)

    url = url_db_handler.get_url_name(connection, id)
    if url is None:
        url_db_handler.close_connection(connection=connection)
        flask.abort(500)

    if not url:
        url_db_handler.close_connection(connection=connection)
        flask.abort(404)

    response = site_processing.get_site_response(url)
    if response is None:
        url_db_handler.close_connection(connection=connection)
        flask.flash('Произошла ошибка при проверке', INFO_MESSAGE_TYPE)
        return flask.redirect(flask.url_for('get_url', id=id))

    data = site_processing.parse_html_response(response)
    status = url_db_handler.create_check(connection, id, data)
    if status is None:
        url_db_handler.close_connection(connection=connection)
        flask.abort(500)

    url_db_handler.close_connection(connection=connection)
    flask.flash('Страница успешно проверена', SUCCES_MESSAGE_TYPE)
    return flask.redirect(flask.url_for('get_url', id=id))


@app.errorhandler(404)
def page_not_found(error: HTTPException) -> tuple[str, int]:
    """Handle error 404"""
    logging.exception(error)
    return flask.render_template('errors/404.html'), 404


@app.errorhandler(500)
def internal_server_error(error: HTTPException) -> tuple[str, int]:
    """Handle error 500."""
    logging.exception(error)
    return flask.render_template('errors/500.html'), 500
