from page_analyzer.urls import (
    check_url,
    create_url,
    get_list_urls,
    get_specific_url_info,
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

    selection_fields = ['id']
    condition = ('name', url)
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


def test_check_url(fake_connection,
                   fake_select,
                   fake_commit):
    url = 'https://www.example.com'
    table = 'urls'
    selection_fields = ['id']
    codition = ('name', url)

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
    selection_fields = ['id', 'name']
    sorting = ('created_at', 'DESC')

    get_list_urls()
    call_args = fake_select.call_args.kwargs.values()

    assert fake_connection.called
    assert table in call_args
    assert selection_fields in call_args
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
    url_id = '1'
    table = 'urls'
    selection_fields = ['id', 'name', 'created_at']
    condition = ('id', '1')

    get_specific_url_info(url_id)
    call_args = fake_select.call_args.kwargs.values()

    assert fake_connection.called
    assert table in call_args
    assert selection_fields in call_args
    assert condition in call_args
    assert fake_commit.called


def test_get_specific_url_connection_error(fake_bad_connection,
                                           fake_select,
                                           fake_commit):
    url_id = '1'

    data = get_specific_url_info(url_id)

    assert data is None
    assert fake_bad_connection.called
    assert not fake_select.called
    assert not fake_commit.called


def test_get_specific_url_select_error(fake_connection,
                                       fake_bad_select,
                                       fake_commit):
    url_id = '1'

    data = get_specific_url_info(url_id)

    assert data is None
    assert fake_connection.called
    assert fake_bad_select.called
    assert not fake_commit.called


def test_get_specific_url_empty_select(fake_connection,
                                       fake_empty_select,
                                       fake_commit):
    url_id = '1'

    data = get_specific_url_info(url_id)

    assert data == {}
    assert fake_connection.called
    assert fake_empty_select.called
    assert fake_commit.called
