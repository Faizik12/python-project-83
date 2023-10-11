from psycopg2 import sql
import pytest

from page_analyzer.url_db_handler import db_operations


@pytest.fixture()
def connection():
    """Creating a connection that will not be committed."""
    connect = db_operations.open_connection()

    yield connect

    connect.close()  # type: ignore # here the connection is not None


def test_generate_selection_string_seccess(connection):
    table = 'urls'
    selection_fields = [('urls', 'name'), ('urls', 'created_at')]
    distinct = ('urls', 'created_at')

    expected = sql.SQL('SELECT DISTINCT ON ("urls"."created_at") '
                       '"urls"."name","urls"."created_at" FROM "urls"\n')
    expected_string = expected.as_string(connection)

    result = db_operations._generate_selection_string(table=table,
                                                      fields=selection_fields,
                                                      distinct=distinct)
    assert result.as_string(connection) == expected_string


def test_generate_joining_string_seccess(connection):
    table = 'urls'
    joining = (('url_checks', 'url_id'), 'id')

    expected = sql.SQL('LEFT JOIN "url_checks" '
                       'ON "urls"."id" = "url_checks"."url_id"\n')
    expected_string = expected.as_string(connection)

    result = db_operations._generate_joining_string(table=table,
                                                    joining=joining)

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


class TestInsertData:

    def test_insert_data_success(self, connection):
        table = 'urls'
        fields = ['name']
        data = {'name': 'https://www.example.com'}

        result = db_operations.insert_data(connection=connection,
                                           table=table,
                                           fields=fields,
                                           data=data)

        assert result is True

    def test_insert_error(self, connection):
        false_table = 'url'
        fields = ['name']
        data = {'name': 'https://www.example.com'}

        result = db_operations.insert_data(connection=connection,
                                           table=false_table,
                                           fields=fields,
                                           data=data)

        assert result is False


class TestSelectData:

    def test_select_data_success(self, connection):
        table = 'urls'
        selection_fields = [('urls', 'name'), ('url_checks', 'created_at')]
        joining = (('url_checks', 'url_id'), 'id')
        condition = (('urls', 'id'), 1)
        sorting = [(('urls', 'created_at'), 'DESC'),
                   (('url_checks', 'created_at'), 'DESC')]
        distinct = ('urls', 'created_at')

        result_1 = db_operations.select_data(connection=connection,
                                             table=table,
                                             fields=selection_fields,
                                             distinct=distinct,
                                             joining=joining,
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
                                             fields=selection_fields,
                                             joining=joining)

        assert data['name'] in result_2[0]['name']  # type: ignore # not None

    def test_select_data_error(self, connection):
        false_table = 'url'
        selection_fields = [('urls', 'name'), ('url_checks', 'created_at')]
        result_3 = db_operations.select_data(connection=connection,
                                             table=false_table,
                                             fields=selection_fields)

        assert result_3 is None
