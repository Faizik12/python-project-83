from unittest.mock import MagicMock

import page_analyzer.urls
import pytest


@pytest.fixture()
def fake_connection(monkeypatch):
    """Changes page_analyzer.urls.open_connection to mock for tests."""

    mock = MagicMock()

    monkeypatch.setattr(page_analyzer.urls, 'open_connection', mock)

    return mock


@pytest.fixture()
def fake_insert(monkeypatch):
    """Changes page_analyzer.urls.insert_data to mock for tests."""

    mock = MagicMock()

    monkeypatch.setattr(page_analyzer.urls, 'insert_data', mock)

    return mock


@pytest.fixture()
def fake_select(monkeypatch):
    """Changes page_analyzer.urls.select_data to mock for tests."""

    mock = MagicMock()

    monkeypatch.setattr(page_analyzer.urls, 'select_data', mock)

    return mock


@pytest.fixture()
def fake_commit(monkeypatch):
    """Changes page_analyzer.urls.commit_changes to mock for tests."""

    mock = MagicMock()

    monkeypatch.setattr(page_analyzer.urls, 'commit_changes', mock)

    return mock


@pytest.fixture()
def fake_bad_connection(monkeypatch):
    """Changes page_analyzer.urls.open_connection to mock for tests."""

    mock = MagicMock()
    mock.return_value = None

    monkeypatch.setattr(page_analyzer.urls, 'open_connection', mock)

    return mock


@pytest.fixture()
def fake_bad_insert(monkeypatch):
    """Changes page_analyzer.urls.insert_data to mock for tests."""

    mock = MagicMock()
    mock.return_value = False

    monkeypatch.setattr(page_analyzer.urls, 'insert_data', mock)

    return mock


@pytest.fixture()
def fake_bad_select(monkeypatch):
    """Changes page_analyzer.urls.select_data to mock for tests."""

    mock = MagicMock()
    mock.return_value = None

    monkeypatch.setattr(page_analyzer.urls, 'select_data', mock)

    return mock


@pytest.fixture()
def fake_empty_select(monkeypatch):
    """Changes page_analyzer.urls.select_data to mock for tests."""

    mock = MagicMock()
    mock.return_value = []

    monkeypatch.setattr(page_analyzer.urls, 'select_data', mock)

    return mock


@pytest.fixture()
def fakeclient(monkeypatch):

    def fake_get(url):
        return url

    monkeypatch.setattr(page_analyzer.urls.requests, 'get', fake_get)


@pytest.fixture()
def fakeresponse():
    class FakeResponse:
        def __init__(self):
            with open('tests/fixtures/sample.html') as file:
                html = file.read()
            self.text = html
            self.status_code = 200

    return FakeResponse
