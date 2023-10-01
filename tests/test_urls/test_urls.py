import datetime

from page_analyzer.urls import (
    _normalize_created_at,
    check_url,
    create_check,
    create_url,
    extract_necessary_data,
    get_list_urls,
    get_site_response,
    get_specific_url_info,
    get_url_name,
    normalize_url,
    validate_url,
)


def test_validate():
    url = 'https://www.example.com/'
    empty_url = ''
    too_long_url = 'http://' + (255 * 'a') + '.com'
    bad_url = 'www.example.com'

    assert not validate_url(url)

    error_empty_url = validate_url(empty_url)
    assert error_empty_url == 'URL обязателен'

    error_too_long_url = validate_url(too_long_url)
    assert error_too_long_url == 'URL превышает 255 символов'

    error_bad_url = validate_url(bad_url)
    assert error_bad_url == 'Некорректный URL'


def test_normalize_url():
    non_normalized_url = 'hTTps://WwW.eXAmple.Com/'
    normalized_url = 'https://www.example.com'

    assert normalized_url == normalize_url(non_normalized_url)


def test_create_url(fake_connection,
                    fake_insert,
                    fake_select,
                    fake_commit):
    url = 'https://www.example.com'
    inserted_data = {'name': url}
    table = 'urls'
    insertion_fields = ['name']

    create_url(url)
    insert_call_args = fake_insert.call_args.kwargs.values()

    assert fake_connection.called
    assert fake_insert.called
    assert table in insert_call_args
    assert insertion_fields in insert_call_args
    assert inserted_data in insert_call_args

    selection_fields = [('urls', 'id')]
    condition = (('urls', 'name'), url)
    select_call_args = fake_select.call_args.kwargs.values()

    assert fake_select.called
    assert table in select_call_args
    assert selection_fields in select_call_args
    assert condition in select_call_args
    assert fake_commit.called


def test_create_url_connection_error(fake_bad_connection,
                                     fake_insert,
                                     fake_select,
                                     fake_commit):
    url = 'https://www.example.com'

    status = create_url(url)

    assert status is None
    assert fake_bad_connection.called
    assert not fake_insert.called
    assert not fake_select.called
    assert not fake_commit.called


def test_create_url_insert_error(fake_connection,
                                 fake_bad_insert,
                                 fake_select,
                                 fake_commit):
    url = 'https://www.example.com'

    status = create_url(url)

    assert status is None
    assert fake_connection.called
    assert fake_bad_insert.called
    assert not fake_select.called
    assert not fake_commit.called


def test_create_url_select_error(fake_connection,
                                 fake_insert,
                                 fake_bad_select,
                                 fake_commit):
    url = 'https://www.example.com'

    status = create_url(url)

    assert status is None
    assert fake_connection.called
    assert fake_insert.called
    assert fake_bad_select.called
    assert not fake_commit.called


def test_create_check(fake_connection,
                      fake_insert,
                      fake_commit):
    url_id = 1
    data = {'status_code': 200,
            'h1': 'h1',
            'title': 'title',
            'description': 'description'}
    table = 'url_checks'
    insertion_fields = ['url_id', 'status_code', 'h1',
                        'title', 'description']
    inserted_data = {'url_id': url_id,
                     'status_code': 200,
                     'h1': 'h1',
                     'title': 'title',
                     'description': 'description'}

    create_check(url_id=url_id, data=data)
    insert_call_args = fake_insert.call_args.kwargs.values()

    assert fake_connection.called
    assert fake_insert.called
    assert table in insert_call_args
    assert insertion_fields in insert_call_args
    assert inserted_data in insert_call_args
    assert fake_commit.called


def test_create_check_connection_error(fake_bad_connection,
                                       fake_insert,
                                       fake_commit):
    url_id = 1
    data = {'status_code': 200,
            'h1': 'h1',
            'title': 'title',
            'description': 'description'}

    status = create_check(url_id=url_id, data=data)

    assert status is None
    assert fake_bad_connection.called
    assert not fake_insert.called
    assert not fake_commit.called


def test_create_check_insert_error(fake_connection,
                                   fake_bad_insert,
                                   fake_commit):
    url_id = 1
    data = {'status_code': 200,
            'h1': 'h1',
            'title': 'title',
            'description': 'description'}

    status = create_check(url_id=url_id, data=data)

    assert status is None
    assert fake_connection.called
    assert fake_bad_insert.called
    assert not fake_commit.called


def test_check_url(fake_connection,
                   fake_select,
                   fake_commit):
    url = 'https://www.example.com'
    table = 'urls'
    selection_fields = [('urls', 'id')]
    codition = (('urls', 'name'), url)

    check_url(url)
    call_args = fake_select.call_args.kwargs.values()

    assert fake_connection.called
    assert table in call_args
    assert selection_fields in call_args
    assert codition in call_args
    assert fake_commit.called


def test_check_url_connection_error(fake_bad_connection,
                                    fake_select,
                                    fake_commit):
    url = 'https://www.example.com'

    data = check_url(url)

    assert data is None
    assert fake_bad_connection.called
    assert not fake_select.called
    assert not fake_commit.called


def test_check_url_select_error(fake_connection,
                                fake_bad_select,
                                fake_commit):
    url = 'https://www.example.com'

    data = check_url(url)

    assert data is None
    assert fake_connection.called
    assert fake_bad_select.called
    assert not fake_commit.called


def test_check_url_empty_select(fake_connection,
                                fake_empty_select,
                                fake_commit):
    url = 'https://www.example.com'

    data = check_url(url)

    assert data == 0
    assert fake_connection.called
    assert fake_empty_select.called
    assert fake_commit.called


def test_get_list_urls(fake_connection,
                       fake_select,
                       fake_commit):
    table = 'urls'
    selection_fields = [('urls', 'id'),
                        ('urls', 'name'),
                        ('url_checks', 'created_at'),
                        ('url_checks', 'status_code')]
    distinct = ('urls', 'created_at')
    joining = (('url_checks', 'url_id'), 'id')
    sorting = [(('urls', 'created_at'), 'DESC'),
               (('url_checks', 'created_at'), 'DESC')]

    get_list_urls()
    call_args = fake_select.call_args.kwargs.values()

    assert fake_connection.called
    assert table in call_args
    assert selection_fields in call_args
    assert distinct in call_args
    assert joining in call_args
    assert sorting in call_args
    assert fake_commit.called


def test_get_list_urls_connection_error(fake_bad_connection,
                                        fake_select,
                                        fake_commit):
    data = get_list_urls()

    assert data is None
    assert fake_bad_connection.called
    assert not fake_select.called
    assert not fake_commit.called


def test_get_list_urls_select_error(fake_connection,
                                    fake_bad_select,
                                    fake_commit):
    data = get_list_urls()

    assert data is None
    assert fake_connection.called
    assert fake_bad_select.called
    assert not fake_commit.called


def test_get_list_urls_empty_select(fake_connection,
                                    fake_empty_select,
                                    fake_commit):
    data = get_list_urls()

    assert data == []
    assert fake_connection.called
    assert fake_empty_select.called
    assert fake_commit.called


def test_get_specific_url_info(fake_connection,
                               fake_select,
                               fake_commit):
    url_id = 1
    first_selection_table = 'urls'
    first_selection_fields = [('urls', 'id'),
                              ('urls', 'name'),
                              ('urls', 'created_at')]
    first_selection_condition = (('urls', 'id'), 1)

    second_selection_table = 'url_checks'
    second_selection_fields = [('url_checks', 'id'),
                               ('url_checks', 'status_code'),
                               ('url_checks', 'h1'),
                               ('url_checks', 'title'),
                               ('url_checks', 'description'),
                               ('url_checks', 'created_at')]
    second_selection_condition = (('url_checks', 'url_id'), 1)

    get_specific_url_info(url_id)
    call_args_list = fake_select.call_args_list
    first_call_args = call_args_list[0].kwargs.values()
    assert fake_connection.called
    assert first_selection_table in first_call_args
    assert first_selection_fields in first_call_args
    assert first_selection_condition in first_call_args

    second_call_args = call_args_list[1].kwargs.values()
    assert second_selection_table in second_call_args
    assert second_selection_fields in second_call_args
    assert second_selection_condition in second_call_args
    assert fake_commit.called


def test_get_specific_url_connection_error(fake_bad_connection,
                                           fake_select,
                                           fake_commit):
    url_id = 1

    data = get_specific_url_info(url_id)

    assert data is None
    assert fake_bad_connection.called
    assert not fake_select.called
    assert not fake_commit.called


def test_get_specific_url_select_error(fake_connection,
                                       fake_bad_select,
                                       fake_commit):
    url_id = 1

    data = get_specific_url_info(url_id)

    assert data is None
    assert fake_connection.called
    assert fake_bad_select.called
    assert not fake_commit.called


def test_get_specific_url_empty_select(fake_connection,
                                       fake_empty_select,
                                       fake_commit):
    url_id = 1

    data = get_specific_url_info(url_id)

    assert data == ({}, [])
    assert fake_connection.called
    assert fake_empty_select.called
    assert fake_commit.called


def test_get_url(fake_connection,
                 fake_select,
                 fake_commit):
    url_id = 1
    table = 'urls'
    selection_fields = [('urls', 'name')]
    codition = (('urls', 'id'), url_id)

    get_url_name(url_id)
    call_args = fake_select.call_args.kwargs.values()

    assert fake_connection.called
    assert table in call_args
    assert selection_fields in call_args
    assert codition in call_args
    assert fake_commit.called


def test_get_url_connection_error(fake_bad_connection,
                                  fake_select,
                                  fake_commit):
    url_id = 1

    data = get_url_name(url_id)

    assert data is None
    assert fake_bad_connection.called
    assert not fake_select.called
    assert not fake_commit.called


def test_get_url_select_error(fake_connection,
                              fake_bad_select,
                              fake_commit):
    url_id = 1

    data = get_url_name(url_id)

    assert data is None
    assert fake_connection.called
    assert fake_bad_select.called
    assert not fake_commit.called


def test_get_url_empty_select(fake_connection,
                              fake_empty_select,
                              fake_commit):
    url_id = 1

    data = get_url_name(url_id)

    assert data == ''
    assert fake_connection.called
    assert fake_empty_select.called
    assert fake_commit.called


def test__normalizer_created_at():
    data = [
        {'created_at': datetime.datetime(2000, 1, 1)},
        {'created_at': datetime.datetime(2020, 2, 2)}]

    _normalize_created_at(data)

    assert isinstance(data[0]['created_at'], datetime.date)
    assert isinstance(data[1]['created_at'], datetime.date)


def test_get_site_response(fakeclient):
    url = 'http://example.com'
    get_site_response(url)

    assert fakeclient.called


def test_extract_necessary_data(fakeresponse):
    response = fakeresponse()
    result_data = {'status_code': 200,
                   'h1': 'Example - simple html',
                   'title': 'Example',
                   'description': 'Example — just html to check'}

    result = extract_necessary_data(response)

    assert result_data == result
