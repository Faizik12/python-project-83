from unittest.mock import MagicMock

import pytest
import requests

from page_analyzer import webutils


class FakeResponse:
    def __init__(self, bad=False):
        with open('tests/fixtures/sample.html') as file:
            html = file.read()
        self.text = html
        self.status_code = 200
        self.bad = bad

    def raise_for_status(self):
        if self.bad:
            raise requests.RequestException
        else:
            pass


@pytest.fixture()
def client(monkeypatch):
    mock = MagicMock()
    mock.return_value = FakeResponse()
    monkeypatch.setattr('page_analyzer.webutils.requests.get', mock)
    return mock


@pytest.fixture()
def bad_client(monkeypatch):
    mock = MagicMock()
    mock.return_value = FakeResponse(bad=True)
    monkeypatch.setattr('page_analyzer.webutils.requests.get', mock)
    return mock


@pytest.fixture()
def fakeresponse():
    return FakeResponse()


class TestGetSiteResponse:
    url = 'http://example.com'

    def test_get_site_response_success(self, client):
        result = webutils.get_site_response(self.url)

        assert client.called
        assert self.url in client.call_args.args
        assert isinstance(result, FakeResponse)

    def test_get_site_response_request_error(self, bad_client):
        with pytest.raises(requests.RequestException):
            webutils.get_site_response(self.url)


def test_parse_html_response_success(fakeresponse):
    response = fakeresponse
    result_data = {'status_code': 200,
                   'h1': 'Example - simple html',
                   'title': 'Example',
                   'description': 'Example — just html to check'}

    result = webutils.parse_html_response(response)

    assert result_data == result
