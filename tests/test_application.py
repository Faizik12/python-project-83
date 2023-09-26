from unittest.mock import MagicMock

import pytest
from page_analyzer import app


@pytest.fixture()
def client():
    """Сreating a test client."""
    test_app = app
    test_app.config.update({
        'TESTING': True,
    })

    return test_app.test_client()


@pytest.fixture(autouse=True)
def fake_funcs(monkeypatch):
    """Changes page_analyzer.urls for all tests to test the app."""
    fake_get_list = MagicMock()
    fake_get_list.return_value = [
        {'id': '2', 'name': 'https://example.com'},
        {'id': '1', 'name': 'http://example.com'}]
    monkeypatch.setattr('page_analyzer.application.get_list_urls',
                        fake_get_list)

    fake_get_specific = MagicMock()
    fake_get_specific.return_value = {
        'id': '1',
        'name': 'http://example.com',
        'created_at': '2000-01-01'}
    monkeypatch.setattr('page_analyzer.application.get_specific_url_info',
                        fake_get_specific)

    fake_create = MagicMock()
    fake_create.return_value = 1
    monkeypatch.setattr('page_analyzer.application.create_url',
                        fake_create)

    fake_check = MagicMock()
    fake_check.return_value = 0
    monkeypatch.setattr('page_analyzer.application.check_url',
                        fake_check)


@pytest.fixture()
def empty_list_urls(monkeypatch):
    """Changes urls.get_list_urls to check for empty list output."""
    fake_get_list = MagicMock()
    fake_get_list.return_value = []
    monkeypatch.setattr('page_analyzer.application.get_list_urls',
                        fake_get_list)


@pytest.fixture()
def incorrect_url_identifier(monkeypatch):
    """Changes urls.get_specific_url_info to check for invalid url id output."""
    fake_get_specific = MagicMock()
    fake_get_specific.return_value = {}
    monkeypatch.setattr('page_analyzer.application.get_specific_url_info',
                        fake_get_specific)


@pytest.fixture()
def existing_url(monkeypatch):
    """Changes urls.check_url to check for invalid url identifier output."""
    fake_check = MagicMock()
    fake_check.return_value = 1
    monkeypatch.setattr('page_analyzer.application.check_url',
                        fake_check)


def test_get_main_page(client):
    response = client.get('/')

    with open('tests/fixtures/index.html') as file:
        form = file.read()

    assert form in response.text


def test_post_urls(client):
    response = client.post('/urls',
                           data={'url': 'http://example.com'},
                           follow_redirects=True)

    assert len(response.history) == 1
    assert response.request.path == '/urls/1'


def test_post_urls_exicting_url(client, existing_url):
    response = client.post('/urls',
                           data={'url': 'http://example.com'},
                           follow_redirects=True)

    assert len(response.history) == 1
    assert response.request.path == '/urls/1'


def test_empty_url_form(client):
    response = client.post('/urls', data={'url': ''})

    with open('tests/fixtures/flash_empty_url.html') as file:
        flash_message = file.read()

    assert response.status_code == 422
    assert flash_message in response.text


def test_too_long_url_form(client):
    too_long_url = 'http://' + (255 * 'a') + '.com'
    response = client.post('/urls', data={'url': too_long_url})
    user_value = f'value="{too_long_url}"'

    with open('tests/fixtures/flash_too_long_url.html') as file:
        flash_message = file.read()

    assert response.status_code == 422
    assert flash_message in response.text
    assert user_value in response.text


def test_bad_url_form(client):
    bad_url = 'example.com'
    response = client.post('/urls', data={'url': bad_url})
    user_value = f'value="{bad_url}"'

    with open('tests/fixtures/flash_bad_url.html') as file:
        flash_message = file.read()

    assert response.status_code == 422
    assert flash_message in response.text
    assert user_value in response.text


def test_get_list_urls(client):
    response = client.get('/urls')

    with open('tests/fixtures/list_urls.html') as file:
        list_urls = file.read()

    assert list_urls in response.text


def test_get_empty_list_urls(client, empty_list_urls):
    response = client.get('/urls')

    with open('tests/fixtures/empty_list_urls.html') as file:
        empty_list_urls = file.read()

    assert empty_list_urls in response.text


def test_get_url(client):
    response = client.get('/urls/1')

    with open('tests/fixtures/url_page.html') as file:
        url_page = file.read()

    assert url_page in response.text


def test_get_url_404_error(client, incorrect_url_identifier):
    response = client.get('/urls/1')

    assert response.status_code == 404
