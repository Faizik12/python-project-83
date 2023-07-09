import pytest
from page_analyzer import app


@pytest.fixture()
def app_testing():
    app.config.update({
        "TESTING": True,
    })
    yield app


@pytest.fixture
def client(app_testing):
    return app_testing.test_client()


def test_render(client):
    response = client.get('/')
    assert b'<h2>Hello, World!</h2>' in response.data
