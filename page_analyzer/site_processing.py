import logging
from typing import Any

import bs4
import requests

SITE_CONNECTION_MESSAGE = 'The response from the site was received'


def get_site_response(url: str) -> requests.Response | None:
    """Execute a request to the site, return a response."""
    try:
        response = requests.get(url, timeout=1)
        response.raise_for_status()
    except requests.RequestException as error:
        logging.exception(error)
        return None

    logging.info(SITE_CONNECTION_MESSAGE)
    return response


def parse_html_response(response: requests.Response) -> dict[str, Any]:
    """Parse the site's response, return the dict with the response data."""
    content = response.text
    status_code = response.status_code
    result: dict[str, Any] = {'status_code': status_code}

    html_tree = bs4.BeautifulSoup(content, 'html.parser')

    tag_h1 = html_tree.h1
    tag_title = html_tree.title
    tag_meta_desc = html_tree.find('meta', attrs={'name': 'description'})

    content_h1: str | None = None
    content_title: str | None = None
    content_desc: str | None = None

    if tag_h1 is not None:
        content_h1 = str(tag_h1.string)
    if tag_title is not None:
        content_title = str(tag_title.string)
    if tag_meta_desc is not None:
        content_desc = str(tag_meta_desc.get('content'))  # type: ignore

    matches_fields = ('h1', 'title', 'description')
    matches_value = (content_h1, content_title, content_desc)
    matches = {key: value if value is not None else None
               for key, value in zip(matches_fields, matches_value)}
    result.update(matches)

    return result
