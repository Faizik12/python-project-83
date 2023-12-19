from __future__ import annotations

import datetime
import logging
import typing as t

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

if t.TYPE_CHECKING:
    from psycopg2.extensions import connection
    from psycopg2.extras import RealDictRow
    from psycopg2.sql import Composed
    from psycopg2.sql import SQL


COMPLETE_OPERATION_MESSAGE = 'The {operation} is completed'
ERROR_OPERATION_MESSAGE = 'Error when trying to {operation}'
CLOSE_CONNECTION_MESSAGE = 'The changes are committed and '\
                           'the connection to the database is close'


def open_connection(db_url: str) -> connection:
    """Create a DB connection, return a connection instance."""
    try:
        conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor)
    except psycopg2.Error:
        logging.exception(ERROR_OPERATION_MESSAGE.format(
            operation='DB connection'))
        raise

    logging.info(COMPLETE_OPERATION_MESSAGE.format(operation='DB connection'))
    return conn


def insert_data(connection: connection,
                table: str,
                fields: list[str],
                data: dict[str, t.Any],
                returning: list[str] | None = None,
                ) -> list[RealDictRow] | None:
    """Insert data into the DB, return None or inserted data."""
    fields = [*fields, 'created_at']
    created_at = datetime.datetime.now()
    data_copy = data.copy()
    data_copy.update(created_at=created_at)

    query = sql.SQL("INSERT INTO {table} ({fields}) "
                    "VALUES ({data})").format(
                        table=sql.Identifier(table),
                        fields=sql.SQL(',').join(map(sql.Identifier, fields)),
                        data=sql.SQL(', ').join(map(sql.Placeholder, fields)))
    query_end = sql.SQL(';')

    if returning is not None:
        returning_string = sql.SQL(' RETURNING {joining_fields}').format(
            joining_fields=sql.SQL(',').join(
                sql.Identifier(field) for field in returning))
        query += returning_string

    result_query = query + query_end
    inserted_data: list[RealDictRow] | None = None

    try:
        with connection.cursor() as cursor:
            cursor.execute(result_query, data_copy)
            if returning is not None:
                inserted_data = cursor.fetchall()  # type: ignore
    except psycopg2.Error:
        logging.exception(ERROR_OPERATION_MESSAGE.format(
            operation='insert data'))
        raise

    logging.info(COMPLETE_OPERATION_MESSAGE.format(operation='insert data'))
    return inserted_data


def select_data(connection: connection,
                table: str,
                fields: list[tuple[str, str]],
                distinct: tuple[str, str] | None = None,
                filtering: tuple[tuple[str, str], str | int] | None = None,
                sorting: list[tuple[tuple[str, str], str]] | None = None,
                ) -> list[RealDictRow]:
    """Select data from the DB, return records list."""
    query = _generate_selection_string(table=table,
                                       fields=fields,
                                       distinct=distinct)
    query_end = sql.SQL(';')

    if filtering is not None:
        filtering_string = _generate_filtering_string(filtering=filtering)
        query += filtering_string

    if sorting is not None:
        sorting_string = _generate_sorting_string(sorting=sorting)
        query += sorting_string

    result_query = query + query_end

    try:
        with connection.cursor() as cursor:
            cursor.execute(result_query)
            data: list[RealDictRow] = cursor.fetchall()  # type: ignore
    except psycopg2.Error:
        logging.exception(ERROR_OPERATION_MESSAGE.format(
            operation='select data'))
        raise

    logging.info(COMPLETE_OPERATION_MESSAGE.format(operation='select data'))
    return data


def close_connection(connection: connection) -> None:
    """Commit all pending transactions and close the connection."""
    connection.commit()
    connection.close()
    logging.info(CLOSE_CONNECTION_MESSAGE)


def _generate_selection_string(table: str,
                               fields: list[tuple[str, str]],
                               distinct: tuple[str, str] | None = None,
                               ) -> Composed:
    """Generate a SQL SELECT query string."""
    string_pattern = 'SELECT{distinct} {fields} FROM {table}\n'
    selection_fields = sql.SQL(',').join(
        sql.Identifier(table) + sql.SQL('.') + sql.Identifier(field)
        for table, field in fields)

    distinct_string: Composed | SQL
    if distinct:
        distinct_string = sql.SQL(' DISTINCT ON ({table}.{field})').format(
            table=sql.Identifier(distinct[0]),
            field=sql.Identifier(distinct[1]))
    else:
        distinct_string = sql.SQL('')

    query = sql.SQL(string_pattern).format(
        table=sql.Identifier(table),
        fields=selection_fields,
        distinct=distinct_string)

    return query


def _generate_filtering_string(filtering: tuple[tuple[str, str], t.Any],
                               ) -> Composed:
    """Generate SQL filtering string."""
    string_pattern = 'WHERE {table}.{field} = {id}\n'
    filtering_table = sql.Identifier(filtering[0][0])
    filtering_field = sql.Identifier(filtering[0][1])
    entry_id = sql.Literal(filtering[1])

    filtering_string = sql.SQL(string_pattern).format(
        table=filtering_table,
        field=filtering_field,
        id=entry_id)
    return filtering_string


def _generate_sorting_string(sorting: list[tuple[tuple[str, str], str]]
                             ) -> Composed:
    """Generate SQL sorting string."""
    string_pattern = '{table}.{field} {order}'
    sorting_fields = []

    for (table, field), order in sorting:
        sorting_fields.append(sql.SQL(string_pattern).format(
            table=sql.Identifier(table),
            field=sql.Identifier(field),
            order=sql.SQL(order)
        ))

    sorting_string = sql.SQL('ORDER BY {sorting_fields}').format(
        sorting_fields=sql.SQL(',').join(sorting_fields))
    return sorting_string
