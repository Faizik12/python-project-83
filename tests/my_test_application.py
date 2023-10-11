from datetime import date
import os
from unittest.mock import MagicMock

import flask
import psycopg2.extras
import pytest

import page_analyzer


def get_fixture_path(name):
    return os.path.join(os.getcwd(), 'tests', 'fixtures', name)


def get_fixture_html(name):
    with open(get_fixture_path(name)) as file:
        html = file.read()
    return html


class FakeResponse:
    def __init__(self):
        with open('tests/fixtures/sample.html') as file:
            html = file.read()
        self.text = html
        self.status_code = 200


@pytest.fixture()
def client():
    test_app = page_analyzer.app
    test_app.config.update({
        'TESTING': True,
    })

    return test_app.test_client()


@pytest.fixture()
def mock_url_db(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr('page_analyzer.application.url_db_handler', mock)
    return mock


@pytest.fixture()
def mock_site_proc(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr('page_analyzer.application.site_processing', mock)
    return mock


def test_index_success(client):
    response = client.get('/')

    form = get_fixture_html('index_form.html')

    assert form in response.text


class TestPostUrl:
    url = '/urls'

    def test_post_urls_success(self, client, mock_url_db):
        mock_url_db.check_url.return_value = 0
        mock_url_db.create_url.return_value = 1
        with client:
            response = client.post(self.url, data={'url': 'http://example.com'})
            message, *_ = flask.get_flashed_messages(with_categories=True)

        assert mock_url_db.open_connection.called
        assert mock_url_db.close_connection.called
        assert response.status_code == 302
        assert response.headers['Location'] == '/urls/1'
        assert message == ('success', 'Страница успешно добавлена')

    def test_post_urls_empty_url(self, client):
        response = client.post(self.url, data={'url': ''})

        flash_messsage = get_fixture_html('flashes/flash_empty_url.html')

        assert response.status_code == 422
        assert flash_messsage in response.text

    def test_post_urls_too_long_url(self, client):
        very_long_url = 'http://' + (255 * 'a') + '.com'
        response = client.post(self.url, data={'url': very_long_url})

        flash_messsage = get_fixture_html('flashes/flash_too_long_url.html')
        user_value = very_long_url

        assert response.status_code == 422
        assert flash_messsage in response.text
        assert user_value in response.text

    def test_post_urls_bad_url(self, client):
        bad_url = 'example.com'
        response = client.post(self.url, data={'url': bad_url})

        flash_messsage = get_fixture_html('flashes/flash_bad_url.html')
        user_value = bad_url

        assert response.status_code == 422
        assert flash_messsage in response.text
        assert user_value in response.text

    def test_post_urls_connection_error(self, client, mock_url_db):
        mock_url_db.open_connection.return_value = None
        response = client.post(self.url,
                               data={'url': 'http://example.com'})

        assert response.status_code == 500

    def test_post_urls_check_url_error(self, client, mock_url_db):
        mock_url_db.check_url.return_value = None
        response = client.post(self.url,
                               data={'url': 'http://example.com'})

        assert mock_url_db.close_connection.called
        assert response.status_code == 500

    def test_post_urls_url_already_exist(self, client, mock_url_db):
        mock_url_db.check_url.return_value = 1
        with client:
            response = client.post(self.url,
                                   data={'url': 'http://example.com'})
            message, *_ = flask.get_flashed_messages(with_categories=True)

        assert mock_url_db.close_connection.called
        assert response.status_code == 302
        assert response.headers['Location'] == '/urls/1'
        assert message == ('info', 'Страница уже существует')

    def test_post_urls_create_url_error(self, client, mock_url_db):
        mock_url_db.check_url.return_value = 0
        mock_url_db.create_url.return_value = None
        response = client.post(self.url,
                               data={'url': 'http://example.com'})

        assert mock_url_db.close_connection.called
        assert response.status_code == 500


class TestGetURLs:
    url = '/urls'

    def test_get_urls_success(self, client, mock_url_db):
        mock_url_db.get_list_urls.return_value = [
            psycopg2.extras.RealDictRow(
                id=2,
                name='https://example.com',
                created_at=date(2000, 1, 2),
                status_code=200),
            psycopg2.extras.RealDictRow(
                id=1,
                name='http://example.com',
                created_at=date(2000, 1, 1),
                status_code=200)]
        response = client.get(self.url)

        list_urls = get_fixture_html('list_urls.html')

        assert mock_url_db.open_connection.called
        assert mock_url_db.close_connection.called
        assert list_urls in response.text

    def test_get_urls_empty_list(self, client, mock_url_db):
        mock_url_db.get_list_urls.return_value = []
        response = client.get(self.url)

        empty_list_urls = get_fixture_html('empty_list_urls.html')

        assert empty_list_urls in response.text

    def test_get_urls_connection_error(self, client, mock_url_db):
        mock_url_db.open_connection.return_value = None
        response = client.get(self.url)

        assert response.status_code == 500

    def test_get_urls_get_list_urls_error(self, client, mock_url_db):
        mock_url_db.get_list_urls.return_value = None
        response = client.get(self.url)

        assert mock_url_db.close_connection.called
        assert response.status_code == 500


class TestGetURL:
    url = '/urls/1'

    def test_get_url_success(self, client, mock_url_db):
        url_data = psycopg2.extras.RealDictRow(
            id=1,
            name='http://example.com',
            created_at=date(2000, 1, 1))
        checks_list = [
            psycopg2.extras.RealDictRow(
                id=2,
                status_code=200,
                h1='h1',
                title='title',
                description='description',
                created_at=date(2000, 1, 2)),
            psycopg2.extras.RealDictRow(
                id=1,
                status_code=200,
                h1='h1',
                title='title',
                description='description',
                created_at=date(2000, 1, 1))]
        mock_url_db.get_specific_url_info.return_value = (url_data, checks_list)
        response = client.get(self.url)

        url_page = get_fixture_html('url_page.html')

        assert mock_url_db.open_connection.called
        assert mock_url_db.close_connection.called
        assert url_page in response.text

    def test_get_url_connection_error(self, client, mock_url_db):
        mock_url_db.open_connection.return_value = None
        response = client.get(self.url)

        assert response.status_code == 500

    def test_get_url_get_specific_url_info_error(self, client, mock_url_db):
        mock_url_db.get_specific_url_info.return_value = None
        response = client.get(self.url)

        assert mock_url_db.close_connection.called
        assert response.status_code == 500

    def test_get_url_non_exist_url(self, client, mock_url_db):
        url_data = psycopg2.extras.RealDictRow()
        mock_url_db.get_specific_url_info.return_value = (url_data, [])
        response = client.get(self.url)

        assert mock_url_db.close_connection.called
        assert response.status_code == 404


class TestPostChecks:
    url = '/urls/1/checks'

    def test_post_checks_success(self, client, mock_url_db, mock_site_proc):
        mock_site_proc.get_site_response.return_value = FakeResponse()
        with client:
            response = client.post(self.url)
            message, *_ = flask.get_flashed_messages(with_categories=True)

        assert mock_url_db.open_connection.called
        assert mock_url_db.close_connection.called
        assert response.status_code == 302
        assert response.headers['Location'] == '/urls/1'
        assert message == ('success', 'Страница успешно проверена')

    def test_post_checks_connection_error(self, client, mock_url_db):
        mock_url_db.open_connection.return_value = None
        response = client.post(self.url)

        assert response.status_code == 500

    def test_post_checks_get_url_name_error(self, client, mock_url_db):
        mock_url_db.get_url_name.return_value = None
        response = client.post(self.url)

        assert mock_url_db.close_connection.called
        assert response.status_code == 500

    def test_post_checks_non_exist_url(self, client, mock_url_db):
        mock_url_db.get_url_name.return_value = ''
        response = client.post(self.url)

        assert mock_url_db.close_connection.called
        assert response.status_code == 404

    def test_post_checks_get_site_response_error(self,
                                                 client,
                                                 mock_url_db,
                                                 mock_site_proc):
        mock_site_proc.get_site_response.return_value = None
        with client:
            response = client.post(self.url)
            message, *_ = flask.get_flashed_messages(with_categories=True)

        assert mock_url_db.close_connection.called
        assert response.status_code == 302
        assert response.headers['Location'] == '/urls/1'
        assert message == ('info', 'Произошла ошибка при проверке')

    def test_post_checks_create_check_error(self,
                                            client,
                                            mock_url_db,
                                            mock_site_proc):
        mock_url_db.create_check.return_value = None
        response = client.post(self.url)

        assert mock_url_db.close_connection.called
        assert response.status_code == 500


def test_page_not_found_succes(client):
    response = client.get('/u')

    error = get_fixture_html('errors/404.html')

    assert error in response.text


def test_internal_server_error_success(client, mock_url_db):
    mock_url_db.open_connection.return_value = None
    response = client.post('/urls',
                           data={'url': 'http://example.com'})

    error = get_fixture_html('errors/500.html')

    assert error in response.text
