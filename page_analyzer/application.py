from __future__ import annotations

import logging
import os
from typing import NoReturn, TYPE_CHECKING

import dotenv
from flask import (
    abort,
    flash,
    Flask,
    get_flashed_messages,
    redirect,
    render_template,
    request,
    url_for
)

from page_analyzer import webutils
from page_analyzer import url_db
from page_analyzer import urlutils

if TYPE_CHECKING:
    from werkzeug.exceptions import HTTPException
    from werkzeug.wrappers import Response

dotenv.load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY')
DATABASE_URL = os.getenv('DATABASE_URL', '')

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
    messages = get_flashed_messages(with_categories=True)
    return render_template('index.html', messages=messages)


@app.post('/urls')
def post_urls() -> Response | tuple[str, int] | NoReturn:
    """Process a request to create a url record."""
    url = request.form.get('url', '')
    error = urlutils.validate_url(url)

    if error:
        flash(error, WARNING_MESSAGE_TYPE)
        messages = get_flashed_messages(with_categories=True)
        return render_template('index.html',
                               messages=messages,
                               url=url), 422

    normalized_url = urlutils.normalize_url(url)

    connection = url_db.open_connection(DATABASE_URL)
    if connection is None:
        abort(500)

    url_id = url_db.check_url(connection, normalized_url)
    if url_id is None:
        url_db.close_connection(connection)
        abort(500)

    if url_id:
        url_db.close_connection(connection)
        flash('Страница уже существует', INFO_MESSAGE_TYPE)
        return redirect(url_for('get_url', id=url_id))

    url_id = url_db.create_url(connection, normalized_url)
    if url_id is None:
        url_db.close_connection(connection)
        abort(500)

    url_db.close_connection(connection)
    flash('Страница успешно добавлена', SUCCES_MESSAGE_TYPE)
    return redirect(url_for('get_url', id=url_id))


@app.get('/urls')
def get_urls() -> str | NoReturn:
    """Return the page with the list of URLs."""
    connection = url_db.open_connection(DATABASE_URL)
    if connection is None:
        abort(500)

    urls = url_db.get_urls(connection)

    if urls is None:
        url_db.close_connection(connection)
        abort(500)

    url_db.close_connection(connection)
    messages = get_flashed_messages(with_categories=True)
    return render_template('urls.html', messages=messages, urls=urls)


@app.get('/urls/<int:id>')
def get_url(id: int) -> str | NoReturn:
    """Return the page to a specific URL."""
    connection = url_db.open_connection(DATABASE_URL)
    if connection is None:
        abort(500)

    url_data = url_db.get_url(connection, id)

    if url_data is None:
        url_db.close_connection(connection)
        abort(500)

    if not url_data:
        url_db.close_connection(connection)
        abort(404)

    checks_list = url_db.get_url_checks(connection, id)

    if checks_list is None:
        url_db.close_connection(connection)
        abort(500)

    url_db.close_connection(connection)
    messages = get_flashed_messages(with_categories=True)
    return render_template('url.html',
                           messages=messages,
                           url=url_data,
                           checks=checks_list)


@app.post('/urls/<int:id>/checks')
def post_checks(id: int) -> Response | NoReturn:
    """Process a request to create a URL verification record."""
    connection = url_db.open_connection(DATABASE_URL)
    if connection is None:
        abort(500)

    url = url_db.get_url(connection, id)
    if url is None:
        url_db.close_connection(connection)
        abort(500)

    if not url:
        url_db.close_connection(connection)
        abort(404)

    response = webutils.get_site_response(url['name'])
    if response is None:
        url_db.close_connection(connection)
        flash('Произошла ошибка при проверке', INFO_MESSAGE_TYPE)
        return redirect(url_for('get_url', id=id))

    page_data = webutils.parse_html_response(response)
    status = url_db.create_check(connection, id, page_data)
    if status is None:
        url_db.close_connection(connection)
        abort(500)

    url_db.close_connection(connection)
    flash('Страница успешно проверена', SUCCES_MESSAGE_TYPE)
    return redirect(url_for('get_url', id=id))


@app.errorhandler(404)
def page_not_found(error: HTTPException) -> tuple[str, int]:
    """Handle error 404"""
    logging.exception(error)
    return render_template('errors/404.html'), 404


@app.errorhandler(500)
def internal_server_error(error: HTTPException) -> tuple[str, int]:
    """Handle error 500."""
    logging.exception(error)
    return render_template('errors/500.html'), 500
