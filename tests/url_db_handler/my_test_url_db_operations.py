from datetime import date
from datetime import datetime
from unittest.mock import MagicMock

import psycopg2.extras
import pytest

from page_analyzer.url_db_handler import url_db_operations


@pytest.fixture()
def mock_db_operations(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(
        'page_analyzer.url_db_handler.url_db_operations.db_operations',
        mock)
    return mock


@pytest.fixture()
def mock_connection():
    return MagicMock()


class TestCreateURL:
    url = 'http://example.com'

    def test_create_url_success(self, mock_db_operations, mock_connection):
        mock_db_operations.insert_data.return_value = True
        mock_db_operations.select_data.return_value = [
            psycopg2.extras.RealDictRow(id=1)]

        table = 'urls'
        inserted_data = {'name': self.url}
        insertion_fields = ['name']

        selection_fields = [('urls', 'id')]
        condition = (('urls', 'name'), self.url)

        result = url_db_operations.create_url(mock_connection, self.url)

        insert_call_args = mock_db_operations.insert_data.call_args
        insert_kwargs = insert_call_args.kwargs.values()

        assert mock_db_operations.insert_data.called
        assert table in insert_kwargs
        assert insertion_fields in insert_kwargs
        assert inserted_data in insert_kwargs

        select_call_args = mock_db_operations.select_data.call_args
        select_kwargs = select_call_args.kwargs.values()

        assert mock_db_operations.select_data.called
        assert table in select_kwargs
        assert selection_fields in select_kwargs
        assert condition in select_kwargs

        assert result == 1

    def test_create_url_insert_error(self, mock_db_operations, mock_connection):
        mock_db_operations.insert_data.return_value = False

        result = url_db_operations.create_url(mock_connection, self.url)

        assert result is None

    def test_create_url_select_error(self, mock_db_operations, mock_connection):
        mock_db_operations.insert_data.return_value = True
        mock_db_operations.select_data.return_value = None

        result = url_db_operations.create_url(mock_connection, self.url)

        assert result is None


class TestCreateURLCheck:

    @pytest.fixture(autouse=True)
    def setup(self):
        data = {
            'status_code': 200,
            'h1': 'Example h1',
            'title': 'Example title',
            'description': 'Example description',
        }
        self.data = data

    def test_create_check_success(self, mock_db_operations, mock_connection):
        mock_db_operations.insert_data.return_value = True

        table = 'url_checks'
        insertion_fields = ['url_id', 'status_code', 'h1',
                            'title', 'description']

        result = url_db_operations.create_check(connection=mock_connection,
                                                url_id=1,
                                                data=self.data)

        insert_call_args = mock_db_operations.insert_data.call_args
        insert_kwargs = insert_call_args.kwargs.values()

        self.data.update(url_id=1)

        assert mock_db_operations.insert_data.called
        assert table in insert_kwargs
        assert insertion_fields in insert_kwargs
        assert self.data in insert_kwargs

        assert result is True

    def test_create_check_insert_error(self,
                                       mock_db_operations,
                                       mock_connection):
        mock_db_operations.insert_data.return_value = False

        result = url_db_operations.create_check(connection=mock_connection,
                                                url_id=1,
                                                data=self.data)

        assert result is None


class TestCheckURL:
    url = 'http://example.com'

    def test_check_url_success(self, mock_db_operations, mock_connection):
        mock_db_operations.select_data.return_value = [
            psycopg2.extras.RealDictRow(id=1)]

        table = 'urls'
        selection_fields = [('urls', 'id')]
        condition = (('urls', 'name'), self.url)

        result = url_db_operations.check_url(mock_connection, self.url)

        select_call_args = mock_db_operations.select_data.call_args
        select_kwargs = select_call_args.kwargs.values()

        assert mock_db_operations.select_data.called
        assert table in select_kwargs
        assert selection_fields in select_kwargs
        assert condition in select_kwargs

        assert result == 1

    def test_check_url_empty_select(self, mock_db_operations, mock_connection):
        mock_db_operations.select_data.return_value = []

        result = url_db_operations.check_url(mock_connection, self.url)

        assert result == 0

    def test_check_url_select_error(self, mock_db_operations, mock_connection):
        mock_db_operations.select_data.return_value = None

        result = url_db_operations.check_url(mock_connection, self.url)

        assert result is None


def test_normalize_created_at():
    record_list = [
        psycopg2.extras.RealDictRow(created_at=datetime(2001, 1, 1, 1, 1, 1)),
        psycopg2.extras.RealDictRow(created_at=datetime(2002, 2, 2, 2, 2, 2))]

    url_db_operations._normalize_created_at(record_list)

    assert record_list[0]['created_at'] == date(2001, 1, 1)
    assert record_list[1]['created_at'] == date(2002, 2, 2)


class TestGetListURLs:

    def test_get_list_urls_success(self, mock_db_operations, mock_connection):
        mock_db_operations.select_data.return_value = [
            psycopg2.extras.RealDictRow(
                id=2,
                name='http://example2.com',
                created_at=datetime(2002, 2, 2, 2, 2, 2),
                status_code=200),
            psycopg2.extras.RealDictRow(
                id=1,
                name='http://example1.com',
                created_at=datetime(2001, 1, 1, 1, 1, 1),
                status_code=200)]

        # Is it possible to make one variable here, pass it to
        # mock.return_value, then modify it with _normalize_created_at
        # and compare the function return to it?
        result_list = [
            psycopg2.extras.RealDictRow(id=2, name='http://example2.com',
                                        created_at=date(2002, 2, 2),
                                        status_code=200),
            psycopg2.extras.RealDictRow(id=1, name='http://example1.com',
                                        created_at=date(2001, 1, 1),
                                        status_code=200)]

        table = 'urls'
        selection_fields = [('urls', 'id'),
                            ('urls', 'name'),
                            ('url_checks', 'created_at'),
                            ('url_checks', 'status_code')]
        distinct = ('urls', 'created_at')
        joining = (('url_checks', 'url_id'), 'id')
        sorting = [(('urls', 'created_at'), 'DESC'),
                   (('url_checks', 'created_at'), 'DESC')]

        result = url_db_operations.get_list_urls(mock_connection)

        select_call_args = mock_db_operations.select_data.call_args
        select_kwargs = select_call_args.kwargs.values()

        assert mock_db_operations.select_data.called
        assert table in select_kwargs
        assert selection_fields in select_kwargs
        assert distinct in select_kwargs
        assert joining in select_kwargs
        assert sorting in select_kwargs

        assert result == result_list

    def test_get_list_urls_empty_select(self,
                                        mock_db_operations,
                                        mock_connection):
        mock_db_operations.select_data.return_value = []

        result = url_db_operations.get_list_urls(mock_connection)

        assert result == []

    def test_get_list_urls_select_error(self,
                                        mock_db_operations,
                                        mock_connection):
        mock_db_operations.select_data.return_value = None

        result = url_db_operations.get_list_urls(mock_connection)

        assert result is None


class TestGetSpecificURLInfo:
    url_id = 1

    def test_get_specific_url_info_success(self,
                                           mock_db_operations,
                                           mock_connection):
        result_for_first_select = [
            psycopg2.extras.RealDictRow(
                id=1,
                name='http://example.com',
                created_at=datetime(2001, 1, 1, 1, 1, 1))]
        result_for_second_select = [
            psycopg2.extras.RealDictRow(
                id=2,
                status_code=200,
                h1='Example h1',
                title='Example title',
                description='Example description',
                created_at=datetime(2002, 2, 2, 2, 2, 2)),
            psycopg2.extras.RealDictRow(
                id=1,
                status_code=200,
                h1='Example h1',
                title='Example title',
                description='Example description',
                created_at=datetime(2001, 1, 1, 1, 1, 1))]
        mock_db_operations.select_data.side_effect = (result_for_first_select,
                                                      result_for_second_select)

        # Arguments for the first select
        table_for_url = 'urls'
        fields_for_url = [('urls', 'id'),
                          ('urls', 'name'),
                          ('urls', 'created_at')]
        condition_for_url = (('urls', 'id'), self.url_id)

        # Arguments for the second select
        table_for_url_checks = 'url_checks'
        fields_for_checks = [('url_checks', 'id'),
                             ('url_checks', 'status_code'),
                             ('url_checks', 'h1'),
                             ('url_checks', 'title'),
                             ('url_checks', 'description'),
                             ('url_checks', 'created_at')]
        condition_for_checks = (('url_checks', 'url_id'), self.url_id)
        sorting = [(('url_checks', 'created_at'), 'DESC')]

        # This is the same issue as test_get_list_urls_success.
        url_record = psycopg2.extras.RealDictRow(
            id=1,
            name='http://example.com',
            created_at=date(2001, 1, 1))
        list_url_checks = [
            psycopg2.extras.RealDictRow(
                id=2,
                status_code=200,
                h1='Example h1',
                title='Example title',
                description='Example description',
                created_at=date(2002, 2, 2)),
            psycopg2.extras.RealDictRow(
                id=1,
                status_code=200,
                h1='Example h1',
                title='Example title',
                description='Example description',
                created_at=date(2001, 1, 1))]

        result_data = (url_record, list_url_checks)

        result = url_db_operations.get_specific_url_info(mock_connection,
                                                         self.url_id)

        call_args_list = mock_db_operations.select_data.call_args_list
        assert mock_db_operations.select_data.call_count == 2

        first_call_args = call_args_list[0].kwargs.values()
        assert table_for_url in first_call_args
        assert fields_for_url in first_call_args
        assert condition_for_url in first_call_args

        second_call_args = call_args_list[1].kwargs.values()
        assert table_for_url_checks in second_call_args
        assert fields_for_checks in second_call_args
        assert condition_for_checks in second_call_args
        assert sorting in second_call_args

        assert result == result_data

    def test_get_specific_url_info_first_empty_select(self,
                                                      mock_db_operations,
                                                      mock_connection):
        mock_db_operations.select_data.return_value = []

        result = url_db_operations.get_specific_url_info(mock_connection,
                                                         self.url_id)

        assert result == (psycopg2.extras.RealDictRow(), [])

    def test_get_specific_url_info_first_select_error(self,
                                                      mock_db_operations,
                                                      mock_connection):
        mock_db_operations.select_data.return_value = None

        result = url_db_operations.get_specific_url_info(mock_connection,
                                                         self.url_id)

        assert result is None

    def test_get_specific_url_info_second_empty_select(self,
                                                       mock_db_operations,
                                                       mock_connection):
        result_for_first_select = [
            psycopg2.extras.RealDictRow(
                id=1,
                name='http://example.com',
                created_at=datetime(2001, 1, 1, 1, 1, 1))]
        result_for_second_select = []
        mock_db_operations.select_data.side_effect = (result_for_first_select,
                                                      result_for_second_select)

        url_record = psycopg2.extras.RealDictRow(
            id=1,
            name='http://example.com',
            created_at=date(2001, 1, 1))

        result_data = (url_record, [])

        result = url_db_operations.get_specific_url_info(mock_connection,
                                                         self.url_id)

        assert result == result_data

    def test_get_specific_url_info_second_select_error(self,
                                                       mock_db_operations,
                                                       mock_connection):
        mock_db_operations.select_data.side_effect = (MagicMock(), None)

        result = url_db_operations.get_specific_url_info(mock_connection,
                                                         self.url_id)

        assert result is None


class TestGetURLName:
    url_id = 1

    def test_get_url_name_success(self, mock_db_operations, mock_connection):
        mock_db_operations.select_data.return_value = [
            psycopg2.extras.RealDictRow(name='http://example.com')]

        table = 'urls'
        fields = [('urls', 'name')]
        condition = (('urls', 'id'), self.url_id)

        result = url_db_operations.get_url_name(mock_connection, self.url_id)

        select_call_args = mock_db_operations.select_data.call_args
        select_kwargs = select_call_args.kwargs.values()

        assert mock_db_operations.select_data.called
        assert table in select_kwargs
        assert fields in select_kwargs
        assert condition in select_kwargs

        assert result == 'http://example.com'

    def test_get_url_name_empty_select(self,
                                       mock_db_operations,
                                       mock_connection):
        mock_db_operations.select_data.return_value = []

        result = url_db_operations.get_url_name(mock_connection, self.url_id)

        assert result == ''

    def test_get_url_name_select_error(self,
                                       mock_db_operations,
                                       mock_connection):
        mock_db_operations.select_data.return_value = None

        result = url_db_operations.get_url_name(mock_connection, self.url_id)

        assert result is None
