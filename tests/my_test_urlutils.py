from page_analyzer import urlutils


def test_validate_success():
    url = 'https://www.example.com/'
    empty_url = ''
    too_long_url = 'http://' + (255 * 'a') + '.com'
    bad_url = 'www.example.com'

    assert not urlutils.validate_url(url)

    error_empty_url = urlutils.validate_url(empty_url)
    assert error_empty_url == 'URL обязателен'

    error_too_long_url = urlutils.validate_url(too_long_url)
    assert error_too_long_url == 'URL превышает 255 символов'

    error_bad_url = urlutils.validate_url(bad_url)
    assert error_bad_url == 'Некорректный URL'


def test_normalize_url_success():
    non_normalized_url = 'hTTps://WwW.eXAmple.Com/example'
    normalized_url = 'https://www.example.com'

    assert normalized_url == urlutils.normalize_url(non_normalized_url)
