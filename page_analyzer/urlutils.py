import urllib.parse

import validators


def validate_url(url: str) -> list[str]:
    """Validate URL address, return error if any."""
    error = []
    if not url:
        error.append('URL обязателен')
    elif len(url) > 255:
        error.append('URL превышает 255 символов')
    elif not validators.url(url):
        error.append('Некорректный URL')
    return error


def normalize_url(source_url: str) -> str:
    """Return URL in normal form."""
    url = urllib.parse.urlparse(source_url)
    return f'{url.scheme}://{url.hostname}'
