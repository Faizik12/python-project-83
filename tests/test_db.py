import pytest
from page_analyzer.db import insert_data, open_connection, select_data


@pytest.fixture()
def connection():
    """Creating a connection that will not be committed."""
    connect = open_connection()

    yield connect

    connect.close()  # type: ignore # here the connection is not None


def test_insert(connection):
    table = 'urls'
    false_table = 'url'
    fields = ['name']
    data = {'name': 'https://www.example.com'}

    assert insert_data(connection, table, fields, data)

    assert not insert_data(connection, false_table, fields, data)


def test_select(connection):
    table = 'urls'
    selection_fields = ['name', 'created_at']
    filter_ = ('id', '1')
    sort = ('created_at', 'DESC')

    result_1 = select_data(
        connection,
        table,
        selection_fields,
        filter_=filter_,
        sorting=sort,
    )
    assert result_1 == []

    insertion_fields = ['name']
    data = {'name': 'https://www.example.com'}
    insert_data(connection, table, insertion_fields, data)

    result_3 = select_data(connection, table, selection_fields)
    assert data['name'] in result_3[0]['name']  # type: ignore # result not bool

    false_table = 'url'
    assert not select_data(connection, false_table, selection_fields)
