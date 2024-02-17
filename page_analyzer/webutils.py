import logging
import typing as t

import bs4
import requests


def get_site_response(url: str) -> requests.Response:
    """Execute a request to the site, return a response."""
    try:
        response = requests.get(url, timeout=1)
        response.raise_for_status()
    except requests.RequestException:
        logging.exception('Error when requesting the site')
        raise

    logging.info('The response from the site was received')
    return response


def parse_html_response(response: requests.Response) -> dict[str, t.Any]:
    """Parse the site's response, return the dict with the response data."""
    content = response.text
    status_code = response.status_code
    data: dict[str, t.Any] = {'status_code': status_code}

    html_tree = bs4.BeautifulSoup(content, 'html.parser')

    tag_desc = html_tree.find('meta', attrs={'name': 'description'})

    if html_tree.h1 is not None:
        data.update(h1=str(html_tree.h1.string))
    else:
        data.update(h1=None)
    if html_tree.title is not None:
        data.update(title=str(html_tree.title.string))
    else:
        data.update(title=None)
    if tag_desc is not None:
        data.update(description=str(tag_desc.get('content')))  # type: ignore
    else:
        data.update(description=None)

    return data
