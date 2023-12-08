from datetime import datetime
import os
from unittest.mock import MagicMock

from flask import get_flashed_messages
import psycopg2.extras
import pytest
import requests

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
    monkeypatch.setattr('page_analyzer.application.url_db', mock)
    return mock


@pytest.fixture()
def mock_webutils(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr('page_analyzer.application.webutils', mock)
    return mock


def test_index_success(client):
    response = client.get('/')

    form = get_fixture_html('index_form.html')

    assert form in response.text


class TestPostUrl:
    url = '/urls'
    form_data = {'url': 'http://example.com'}

    def test_post_urls_success(self, client, mock_url_db):
        mock_url_db.check_url.return_value = None
        mock_url_db.create_url.return_value = 1
        with client:
            response = client.post(self.url, data=self.form_data)
            message, *_ = get_flashed_messages(with_categories=True)

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
        mock_url_db.open_connection.side_effect = psycopg2.Error
        response = client.post(self.url, data=self.form_data)

        assert response.status_code == 500

    def test_post_urls_check_url_error(self, client, mock_url_db):
        mock_url_db.check_url.side_effect = psycopg2.Error
        response = client.post(self.url, data=self.form_data)

        assert mock_url_db.close_connection.called
        assert response.status_code == 500

    def test_post_urls_url_already_exist(self, client, mock_url_db):
        mock_url_db.check_url.return_value = 1
        with client:
            response = client.post(self.url, data=self.form_data)
            message, *_ = get_flashed_messages(with_categories=True)

        assert mock_url_db.close_connection.called
        assert response.status_code == 302
        assert response.headers['Location'] == '/urls/1'
        assert message == ('info', 'Страница уже существует')

    def test_post_urls_create_url_error(self, client, mock_url_db):
        mock_url_db.check_url.return_value = None
        mock_url_db.create_url.side_effect = psycopg2.Error
        response = client.post(self.url, data=self.form_data)

        assert mock_url_db.close_connection.called
        assert response.status_code == 500


class TestGetURLs:
    url = '/urls'

    def test_get_urls_success(self, client, mock_url_db):
        mock_url_db.get_urls.return_value = [
            psycopg2.extras.RealDictRow(
                id=2,
                name='https://example2.com',
                created_at=datetime(2002, 2, 2, 2, 2, 2),
                status_code=200),
            psycopg2.extras.RealDictRow(
                id=1,
                name='http://example1.com',
                created_at=datetime(2001, 1, 1, 1, 1, 1),
                status_code=200)]
        response = client.get(self.url)

        urls = get_fixture_html('urls.html')

        assert mock_url_db.open_connection.called
        assert mock_url_db.close_connection.called
        assert urls in response.text

    def test_get_urls_empty_list(self, client, mock_url_db):
        mock_url_db.get_urls.return_value = []
        response = client.get(self.url)

        empty_list_urls = get_fixture_html('empty_list_urls.html')

        assert empty_list_urls in response.text

    def test_get_urls_connection_error(self, client, mock_url_db):
        mock_url_db.open_connection.side_effect = psycopg2.Error
        response = client.get(self.url)

        assert response.status_code == 500

    def test_get_urls_get_list_urls_error(self, client, mock_url_db):
        mock_url_db.get_urls.side_effect = psycopg2.Error
        response = client.get(self.url)

        assert mock_url_db.close_connection.called
        assert response.status_code == 500


class TestGetURL:
    url = '/urls/1'
    url_data = psycopg2.extras.RealDictRow(
        id=1,
        name='http://example.com',
        created_at=datetime(2000, 1, 1, 1, 1, 1))

    def test_get_url_success(self, client, mock_url_db):
        checks_list = [
            psycopg2.extras.RealDictRow(
                id=2,
                status_code=200,
                h1='h1',
                title='title',
                description='description',
                created_at=datetime(2000, 2, 2, 2, 2, 2)),
            psycopg2.extras.RealDictRow(
                id=1,
                status_code=200,
                h1='h1',
                title='title',
                description='description',
                created_at=datetime(2000, 1, 1, 1, 1, 1))]
        mock_url_db.get_url.return_value = self.url_data
        mock_url_db.get_url_checks.return_value = checks_list
        response = client.get(self.url)

        url_page = get_fixture_html('url_page.html')

        assert mock_url_db.open_connection.called
        assert mock_url_db.close_connection.called
        assert url_page in response.text

    def test_get_url_connection_error(self, client, mock_url_db):
        mock_url_db.open_connection.side_effect = psycopg2.Error
        response = client.get(self.url)

        assert response.status_code == 500

    def test_get_url_get_url_error(self, client, mock_url_db):
        mock_url_db.get_url.side_effect = psycopg2.Error
        response = client.get(self.url)

        assert mock_url_db.close_connection.called
        assert response.status_code == 500

    def test_get_url_non_exist_url(self, client, mock_url_db):
        url_data = None
        mock_url_db.get_url.return_value = url_data
        response = client.get(self.url)

        assert mock_url_db.close_connection.called
        assert response.status_code == 404

    def test_get_url_get_url_checks_error(self, client, mock_url_db):
        mock_url_db.get_url.return_value = self.url_data
        mock_url_db.get_url_checks.side_effect = psycopg2.Error
        response = client.get(self.url)

        assert mock_url_db.close_connection.called
        assert response.status_code == 500


class TestPostChecks:
    url = '/urls/1/checks'

    def test_post_checks_success(self, client, mock_url_db, mock_webutils):
        url_data = psycopg2.extras.RealDictRow(
            id=1,
            name='http://example.com',
            created_at=datetime(2000, 1, 1, 1, 1, 1))
        mock_url_db.get_url.return_value = url_data
        mock_webutils.get_site_response.return_value = FakeResponse()
        with client:
            response = client.post(self.url)
            message, *_ = get_flashed_messages(with_categories=True)

        assert mock_url_db.open_connection.called
        assert mock_url_db.close_connection.called
        assert response.status_code == 302
        assert response.headers['Location'] == '/urls/1'
        assert message == ('success', 'Страница успешно проверена')

    def test_post_checks_connection_error(self, client, mock_url_db):
        mock_url_db.open_connection.side_effect = psycopg2.Error
        response = client.post(self.url)

        assert response.status_code == 500

    def test_post_checks_get_url_error(self, client, mock_url_db):
        mock_url_db.get_url.side_effect = psycopg2.Error
        response = client.post(self.url)

        assert mock_url_db.close_connection.called
        assert response.status_code == 500

    def test_post_checks_non_exist_url(self, client, mock_url_db):
        mock_url_db.get_url.return_value = None
        response = client.post(self.url)

        assert mock_url_db.close_connection.called
        assert response.status_code == 404

    def test_post_checks_get_site_response_error(self,
                                                 client,
                                                 mock_url_db,
                                                 mock_webutils):
        mock_webutils.get_site_response.side_effect = requests.RequestException
        with client:
            response = client.post(self.url)
            message, *_ = get_flashed_messages(with_categories=True)

        assert mock_url_db.close_connection.called
        assert response.status_code == 302
        assert response.headers['Location'] == '/urls/1'
        assert message == ('info', 'Произошла ошибка при проверке')

    def test_post_checks_create_check_error(self,
                                            client,
                                            mock_url_db,
                                            mock_webutils):
        mock_url_db.create_check.side_effect = psycopg2.Error
        response = client.post(self.url)

        assert mock_url_db.close_connection.called
        assert response.status_code == 500


def test_page_not_found_succes(client):
    response = client.get('/u')

    error = get_fixture_html('errors/404.html')

    assert error in response.text


def test_internal_server_error_success(client, mock_url_db):
    mock_url_db.open_connection.side_effect = psycopg2.Error
    response = client.post('/urls',
                           data={'url': 'http://example.com'})

    error = get_fixture_html('errors/500.html')

    assert error in response.text
