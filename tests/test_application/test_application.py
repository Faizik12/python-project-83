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


def test_post_checks(client):
    response = client.post('/urls/1/checks', follow_redirects=True)

    assert len(response.history) == 1
    assert response.request.path == '/urls/1'
