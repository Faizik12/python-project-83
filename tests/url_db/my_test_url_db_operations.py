from datetime import datetime
from unittest.mock import MagicMock

import psycopg2.extras
import pytest

from page_analyzer.url_db import url_db_operations


@pytest.fixture()
def mock_db_operations(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(
        'page_analyzer.url_db.url_db_operations.db_operations',
        mock)
    return mock


@pytest.fixture()
def mock_connection():
    return MagicMock()


class TestMergeURLsChecks:
    urls = [
        psycopg2.extras.RealDictRow(
            id=2,
            name='http://example2.com'),
        psycopg2.extras.RealDictRow(
            id=1,
            name='http://example1.com')]
    url_checks = [
        psycopg2.extras.RealDictRow(
            url_id=2,
            created_at=datetime(2002, 2, 2, 2, 2, 2),
            status_code=200),
        psycopg2.extras.RealDictRow(
            url_id=1,
            created_at=datetime(2001, 1, 1, 1, 1, 1),
            status_code=200)]

    def test_merge_urls_checks_success(self):
        correct_result = [
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

        result = url_db_operations._merge_urls_checks(self.urls,
                                                      self.url_checks)

        assert result == correct_result

    def test_merge_urls_checks_urls_empty(self):
        result = url_db_operations._merge_urls_checks([], self.url_checks)

        assert result == []

    def test_get_urls_second_select_empty(self):
        result = url_db_operations._merge_urls_checks(self.urls, [])

        assert result == self.urls


class TestCreateURL:
    url = 'http://example.com'

    def test_create_url_success(self, mock_db_operations, mock_connection):
        returning_value = [psycopg2.extras.RealDictRow(id=1)]
        mock_db_operations.insert_data.return_value = returning_value

        table = 'urls'
        data = {'name': self.url}
        fields = ['name']
        returning_field = ['id']

        result = url_db_operations.create_url(mock_connection, self.url)

        insert_call_args = mock_db_operations.insert_data.call_args
        insert_kwargs = insert_call_args.kwargs.values()

        assert mock_db_operations.insert_data.called
        assert table in insert_kwargs
        assert fields in insert_kwargs
        assert data in insert_kwargs
        assert returning_field in insert_kwargs

        assert result == 1

    def test_create_url_insert_error(self, mock_db_operations, mock_connection):
        mock_db_operations.insert_data.side_effect = psycopg2.Error

        with pytest.raises(psycopg2.Error):
            url_db_operations.create_url(mock_connection, self.url)


class TestCreateURLCheck:
    check_data = {
        'status_code': 200,
        'h1': 'Example h1',
        'title': 'Example title',
        'description': 'Example description',
    }
    url_id = 1

    def test_create_check_success(self, mock_db_operations, mock_connection):
        mock_db_operations.insert_data.return_value = None

        table = 'url_checks'
        fields = ['url_id', 'status_code', 'h1',
                            'title', 'description']
        data_copy = self.check_data.copy()

        url_db_operations.create_check(connection=mock_connection,
                                       url_id=self.url_id,
                                       data=data_copy)

        insert_call_args = mock_db_operations.insert_data.call_args
        insert_kwargs = insert_call_args.kwargs.values()

        data_copy.update(url_id=self.url_id)

        assert mock_db_operations.insert_data.called
        assert table in insert_kwargs
        assert fields in insert_kwargs
        assert data_copy in insert_kwargs

    def test_create_check_insert_error(self,
                                       mock_db_operations,
                                       mock_connection):
        mock_db_operations.insert_data.side_effect = psycopg2.Error

        with pytest.raises(psycopg2.Error):
            url_db_operations.create_check(connection=mock_connection,
                                           url_id=self.url_id,
                                           data=self.check_data)


class TestCheckURL:
    url = 'http://example.com'

    def test_check_url_success(self, mock_db_operations, mock_connection):
        mock_db_operations.select_data.return_value = [
            psycopg2.extras.RealDictRow(id=1)]

        table = 'urls'
        fields = [('urls', 'id')]
        condition = (('urls', 'name'), self.url)

        result = url_db_operations.check_url(mock_connection, self.url)

        select_call_args = mock_db_operations.select_data.call_args
        select_kwargs = select_call_args.kwargs.values()

        assert mock_db_operations.select_data.called
        assert table in select_kwargs
        assert fields in select_kwargs
        assert condition in select_kwargs

        assert result == 1

    def test_check_url_empty_select(self, mock_db_operations, mock_connection):
        mock_db_operations.select_data.return_value = []

        result = url_db_operations.check_url(mock_connection, self.url)

        assert result is None

    def test_check_url_select_error(self, mock_db_operations, mock_connection):
        mock_db_operations.select_data.side_effect = psycopg2.Error

        with pytest.raises(psycopg2.Error):
            url_db_operations.check_url(mock_connection, self.url)


class TestGetURLs:
    selected_urls = [
        psycopg2.extras.RealDictRow(
            id=2,
            name='http://example2.com'),
        psycopg2.extras.RealDictRow(
            id=1,
            name='http://example1.com')]
    selected_url_checks = [
        psycopg2.extras.RealDictRow(
            url_id=2,
            created_at=datetime(2002, 2, 2, 2, 2, 2),
            status_code=200),
        psycopg2.extras.RealDictRow(
            url_id=1,
            created_at=datetime(2001, 1, 1, 1, 1, 1),
            status_code=200)]

    def test_get_urls_success(self,
                              mock_db_operations,
                              mock_connection):
        mock_db_operations.select_data.side_effect = (self.selected_urls,
                                                      self.selected_url_checks)

        urls_table = 'urls'
        urls_fields = [('urls', 'id'), ('urls', 'name')]
        urls_sorting = [(('urls', 'created_at'), 'DESC')]

        checks_table = 'url_checks'
        checks_fields = [('url_checks', 'created_at'),
                         ('url_checks', 'status_code'),
                         ('url_checks', 'url_id')]
        checks_distinct = ('url_checks', 'url_id')
        checks_sorting = [(('url_checks', 'url_id'), 'ASC'),
                          (('url_checks', 'created_at'), 'DESC')]

        correct_result = [
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

        result = url_db_operations.get_urls(mock_connection)

        call_args = mock_db_operations.select_data.call_args_list
        assert mock_db_operations.select_data.call_count == 2

        first_call_args = call_args[0].kwargs.values()
        assert urls_table in first_call_args
        assert urls_fields in first_call_args
        assert urls_sorting in first_call_args

        second_call_args = call_args[1].kwargs.values()
        assert checks_table in second_call_args
        assert checks_fields in second_call_args
        assert checks_distinct in second_call_args
        assert checks_sorting in second_call_args

        assert result == correct_result

    def test_get_urls_first_select_error(self,
                                         mock_db_operations,
                                         mock_connection):
        mock_db_operations.select_data.side_effect = psycopg2.Error

        with pytest.raises(psycopg2.Error):
            url_db_operations.get_urls(mock_connection)

    def test_get_urls_second_select_error(self,
                                          mock_db_operations,
                                          mock_connection):
        mock_db_operations.select_data.side_effect = (self.selected_urls,
                                                      psycopg2.Error)

        with pytest.raises(psycopg2.Error):
            url_db_operations.get_urls(mock_connection)


class TestGetURLChecks:
    url_id = 1

    def test_get_url_checks_success(self,
                                    mock_db_operations,
                                    mock_connection):
        selection_data = [
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
        mock_db_operations.select_data.return_value = selection_data

        table = 'url_checks'
        fields = [('url_checks', 'id'),
                  ('url_checks', 'status_code'),
                  ('url_checks', 'h1'),
                  ('url_checks', 'title'),
                  ('url_checks', 'description'),
                  ('url_checks', 'created_at')]
        condition = (('url_checks', 'url_id'), self.url_id)
        sorting = [(('url_checks', 'created_at'), 'DESC')]

        result = url_db_operations.get_url_checks(mock_connection, self.url_id)

        select_call_args = mock_db_operations.select_data.call_args
        select_kwargs = select_call_args.kwargs.values()

        assert table in select_kwargs
        assert fields in select_kwargs
        assert condition in select_kwargs
        assert sorting in select_kwargs

        assert result == selection_data

    def test_get_url_checks_empty_select(self,
                                         mock_db_operations,
                                         mock_connection):
        mock_db_operations.select_data.return_value = []

        result = url_db_operations.get_url_checks(mock_connection, self.url_id)

        assert result == []

    def test_get_url_checks_select_error(self,
                                         mock_db_operations,
                                         mock_connection):
        mock_db_operations.select_data.side_effect = psycopg2.Error

        with pytest.raises(psycopg2.Error):
            url_db_operations.get_url_checks(mock_connection, self.url_id)


class TestGetURL:
    url_id = 1

    def test_get_url_success(self, mock_db_operations, mock_connection):
        selection_data = [
            psycopg2.extras.RealDictRow(
                id=1,
                name='http://example.com',
                created_at=datetime(2001, 1, 1, 1, 1, 1))]
        mock_db_operations.select_data.return_value = selection_data

        table = 'urls'
        fields = [('urls', 'id'), ('urls', 'name'), ('urls', 'created_at')]
        condition = (('urls', 'id'), self.url_id)

        result = url_db_operations.get_url(mock_connection, self.url_id)

        select_call_args = mock_db_operations.select_data.call_args
        select_kwargs = select_call_args.kwargs.values()

        assert mock_db_operations.select_data.called
        assert table in select_kwargs
        assert fields in select_kwargs
        assert condition in select_kwargs

        assert result == selection_data[0]

    def test_get_url_empty_select(self,
                                  mock_db_operations,
                                  mock_connection):
        mock_db_operations.select_data.return_value = []

        result = url_db_operations.get_url(mock_connection, self.url_id)

        assert result is None

    def test_get_url_select_error(self,
                                  mock_db_operations,
                                  mock_connection):
        mock_db_operations.select_data.side_effect = psycopg2.Error

        with pytest.raises(psycopg2.Error):
            url_db_operations.get_url(mock_connection, self.url_id)
