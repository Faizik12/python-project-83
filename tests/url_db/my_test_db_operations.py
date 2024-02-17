import os

import dotenv
import psycopg2
from psycopg2 import sql
import pytest

from page_analyzer.url_db import db_operations

dotenv.load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL', '')


@pytest.fixture()
def connection():
    connect = db_operations.open_connection(DATABASE_URL)

    yield connect

    connect.close()


def test_generate_selection_string_seccess(connection):
    table = 'urls'
    fields = [('urls', 'name'), ('urls', 'created_at')]
    distinct = ('urls', 'created_at')

    expected = sql.SQL('SELECT DISTINCT ON ("urls"."created_at") '
                       '"urls"."name","urls"."created_at" FROM "urls"\n')
    expected_string = expected.as_string(connection)

    result = db_operations._generate_selection_string(table=table,
                                                      fields=fields,
                                                      distinct=distinct)
    assert result.as_string(connection) == expected_string


def test_generate_filtering_string_seccess(connection):
    condition = (('urls', 'id'), 1)

    expected = sql.SQL('WHERE "urls"."id" = 1\n')
    expected_string = expected.as_string(connection)

    result = db_operations._generate_filtering_string(filtering=condition)

    assert result.as_string(connection) == expected_string


def test_generate_sorting_string_seccess(connection):
    sorting = [(('urls', 'created_at'), 'DESC'),
               (('url_checks', 'created_at'), 'DESC')]

    expected = sql.SQL('ORDER BY "urls"."created_at" DESC,'
                       '"url_checks"."created_at" DESC')
    expected_string = expected.as_string(connection)

    result = db_operations._generate_sorting_string(sorting=sorting)

    assert result.as_string(connection) == expected_string


def test_open_connection_error():
    with pytest.raises(psycopg2.Error):
        db_operations.open_connection('incorrect DB URL')


def test_close_connection_success(connection):
    db_operations.close_connection(connection)

    assert connection.closed


class TestInsertData:

    def test_insert_data_success(self, connection):
        table = 'urls'
        fields = ['name']
        data = {'name': 'https://www.example.com'}

        returning = db_operations.insert_data(connection=connection,
                                              table=table,
                                              fields=fields,
                                              data=data)

        assert returning is None

    def test_insert_data_success_with_returning(self, connection):
        table = 'urls'
        fields = ['name']
        data = {'name': 'https://www.example.com'}
        returning_field = ['name']

        returning = db_operations.insert_data(connection=connection,
                                              table=table,
                                              fields=fields,
                                              data=data,
                                              returning=returning_field)

        assert data['name'] in returning[0].name  # type: ignore

    def test_insert_error(self, connection):
        false_table = 'url'
        fields = ['name']
        data = {'name': 'https://www.example.com'}

        with pytest.raises(psycopg2.Error):
            db_operations.insert_data(connection=connection,
                                      table=false_table,
                                      fields=fields,
                                      data=data)


class TestSelectData:

    def test_select_data_success(self, connection):
        table = 'urls'
        selection_fields: list[tuple[str, str]]
        selection_fields = [('urls', 'name')]
        condition = (('urls', 'id'), 1)
        sorting: list[tuple[tuple[str, str], str]]
        sorting = [(('urls', 'created_at'), 'DESC'),]
        distinct = ('urls', 'created_at')

        result_1 = db_operations.select_data(connection=connection,
                                             table=table,
                                             fields=selection_fields,
                                             distinct=distinct,
                                             filtering=condition,
                                             sorting=sorting)

        assert result_1 == []

        insertion_fields = ['name']
        data = {'name': 'https://www.example.com'}
        db_operations.insert_data(connection=connection,
                                  table=table,
                                  fields=insertion_fields,
                                  data=data)

        result_2 = db_operations.select_data(connection=connection,
                                             table=table,
                                             fields=selection_fields)

        assert data['name'] in result_2[0].name  # type: ignore

    def test_select_data_error(self, connection):
        false_table = 'url'
        selection_fields: list[tuple[str, str]]
        selection_fields = [('urls', 'name')]

        with pytest.raises(psycopg2.Error):
            db_operations.select_data(connection=connection,
                                      table=false_table,
                                      fields=selection_fields)
