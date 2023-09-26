import logging
import os

from dotenv import load_dotenv
from flask import (
    Flask,
    abort,
    flash,
    get_flashed_messages,
    redirect,
    render_template,
    request,
    url_for,
)
from page_analyzer.urls import (
    check_url,
    create_url,
    get_list_urls,
    get_specific_url_info,
    normalize_url,
    validate_url,
)

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY')

WARNING_MESSAGE_TYPE = 'danger'
SUCCES_MESSAGE_TYPE = 'success'
INFO_MESSAGE_TYPE = 'info'

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] [%(levelname)s] '
                           '[%(module)s.%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


@app.get('/')
def index():
    messages = get_flashed_messages(with_categories=True)
    return render_template('index.html', messages=messages)


@app.post('/urls')
def post_urls():
    url = request.form.get('url')
    error = validate_url(url)  # type: ignore # the url is not None

    if error:
        flash(error, WARNING_MESSAGE_TYPE)
        messages = get_flashed_messages(with_categories=True)
        return render_template('index.html', messages=messages, url=url), 422

    normalized_url = normalize_url(url)  # type: ignore # the url is not None

    url_id = check_url(normalized_url)
    if url_id is None:
        abort(500)

    if url_id:
        flash('Страница уже существует', INFO_MESSAGE_TYPE)
        return redirect(url_for('get_url', id=url_id)), 302

    url_id = create_url(normalized_url)
    if url_id is None:
        abort(500)

    flash('Страница успешно добавлена', SUCCES_MESSAGE_TYPE)
    return redirect(url_for('get_url', id=url_id)), 302


@app.get('/urls')
def get_urls():
    list_urls = get_list_urls()

    if list_urls is None:
        abort(500)

    messages = get_flashed_messages(with_categories=True)
    return render_template('urls.html', messages=messages, urls=list_urls)


@app.get('/urls/<int:id>')
def get_url(id):
    url_info = get_specific_url_info(id)

    if url_info is None:
        abort(500)

    if not url_info:
        abort(404)

    messages = get_flashed_messages(with_categories=True)
    return render_template('url.html', messages=messages, url=url_info)


@app.errorhandler(404)
def page_not_found(error):
    logging.exception(error)
    return render_template('page_not_found.html'), 404


@app.errorhandler(500)
def internal_server_error(error):
    logging.exception(error)
    return render_template('internal_server_error.html'), 500
