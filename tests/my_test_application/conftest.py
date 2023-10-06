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
        {'id': '2', 'name': 'https://example.com',
         'created_at': '2000-01-01', 'status_code': 200},
        {'id': '1', 'name': 'http://example.com',
         'created_at': '2000-01-02', 'status_code': 200}]
    monkeypatch.setattr('page_analyzer.application.get_list_urls',
                        fake_get_list)

    fake_get_specific = MagicMock()
    url_data = {'id': '1',
                'name': 'http://example.com',
                'created_at': '2000-01-01'}
    checks_list = [{'id': 1,
                    'status_code': 200,
                    'h1': 'h1',
                    'title': 'title',
                    'description': 'description',
                    'created_at': '2000-01-01'}]
    fake_get_specific.return_value = (url_data, checks_list)
    monkeypatch.setattr('page_analyzer.application.get_specific_url_info',
                        fake_get_specific)

    fake_create_url = MagicMock()
    fake_create_url.return_value = 1
    monkeypatch.setattr('page_analyzer.application.create_url',
                        fake_create_url)

    fake_check = MagicMock()
    fake_check.return_value = 0
    monkeypatch.setattr('page_analyzer.application.check_url',
                        fake_check)

    fake_get_url_name = MagicMock()
    fake_get_url_name.return_value = 1
    monkeypatch.setattr('page_analyzer.application.get_url_name',
                        fake_get_url_name)

    fake_get_site_response = MagicMock()
    fake_get_site_response.return_value = True
    monkeypatch.setattr('page_analyzer.application.get_site_response',
                        fake_get_site_response)

    fake_extract = MagicMock()
    fake_extract.return_value = {}
    monkeypatch.setattr('page_analyzer.application.extract_necessary_data',
                        fake_extract)

    fake_create_check = MagicMock()
    fake_create_check.return_value = True
    monkeypatch.setattr('page_analyzer.application.create_check',
                        fake_create_check)


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
    fake_get_specific.return_value = [{}, []]
    monkeypatch.setattr('page_analyzer.application.get_specific_url_info',
                        fake_get_specific)


@pytest.fixture()
def existing_url(monkeypatch):
    """Changes urls.check_url to check for invalid url identifier output."""
    fake_check = MagicMock()
    fake_check.return_value = 1
    monkeypatch.setattr('page_analyzer.application.check_url',
                        fake_check)
