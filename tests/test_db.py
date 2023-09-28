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

    assert insert_data(connection=connection,
                       table=table,
                       fields=fields,
                       data=data)

    assert not insert_data(connection=connection,
                           table=false_table,
                           fields=fields,
                           data=data)


def test_select(connection):
    table = 'urls'
    selection_fields = [('urls', 'name'), ('urls', 'created_at')]
    joining = (('url_checks', 'url_id'), 'id')
    condition = (('urls', 'id'), '1')
    sort = [(('urls', 'created_at'), 'DESC'),
            (('urls', 'created_at'), 'DESC')]

    result_1 = select_data(connection=connection,
                           table=table,
                           fields=selection_fields,
                           distinct=('urls', 'created_at'),
                           joining=joining,
                           filtering=condition,
                           sorting=sort)  # type: ignore # sort match the scheme

    assert result_1 == []

    insertion_fields = ['name']
    data = {'name': 'https://www.example.com'}
    insert_data(connection=connection,
                table=table,
                fields=insertion_fields,
                data=data)

    result_2 = select_data(connection=connection,
                           table=table,
                           fields=selection_fields)

    assert data['name'] in result_2[0]['name']  # type: ignore # result not bool

    false_table = 'url'
    assert not select_data(connection=connection,
                           table=false_table,
                           fields=selection_fields)
