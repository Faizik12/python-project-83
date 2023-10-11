from __future__ import annotations

import datetime
import logging
import os
from typing import Any, TYPE_CHECKING

import dotenv
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

if TYPE_CHECKING:
    from psycopg2.extensions import connection
    from psycopg2.extras import RealDictRow
    from psycopg2.sql import Composed
    from psycopg2.sql import SQL

dotenv.load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')

OPEN_CONNECTION_MESSAGE = 'A connection to the database has been established'
CLOSE_CONNECTION_MESSAGE = 'The changes are committed and '\
                           'the connection to the database is close'
COMPLETE_OPERATION_MESSAGE = 'The {operation} operation is completed'


def open_connection() -> connection | None:
    """Create a db connection, return a connection instance or None on error."""
    conn = None

    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    except psycopg2.Error as error:
        logging.exception(error)

    logging.info(OPEN_CONNECTION_MESSAGE)
    return conn


def insert_data(connection: connection,
                table: str,
                fields: list[str],
                data: dict[str, Any],
                ) -> bool:
    """Insert data into the db and return the status of the operation."""
    fields = [*fields, 'created_at']
    created_at = datetime.datetime.now()
    data.update(created_at=created_at)

    query = sql.SQL("INSERT INTO {table} ({fields}) "
                    "VALUES ({data});").format(
                        table=sql.Identifier(table),
                        fields=sql.SQL(',').join(map(sql.Identifier, fields)),
                        data=sql.SQL(', ').join(map(sql.Placeholder, fields)))

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, data)
    except psycopg2.Error as error:
        connection.rollback()
        connection.close()
        logging.exception(error)
        return False

    logging.info(COMPLETE_OPERATION_MESSAGE.format(operation='insert'))
    return True


def select_data(connection: connection,
                table: str,
                fields: list[tuple[str, str]],
                distinct: tuple[str, str] | None = None,
                joining: tuple[tuple[str, str], str] | None = None,
                filtering: tuple[tuple[str, str], str | int] | None = None,
                sorting: list[tuple[tuple[str, str], str]] | None = None,
                ) -> list[RealDictRow] | None:
    """Select data from the db, return records list or None if error occurs."""
    query = _generate_selection_string(table=table,
                                       fields=fields,
                                       distinct=distinct)
    query_end = sql.SQL(';')

    if joining is not None:
        joining_string = _generate_joining_string(table=table, joining=joining)
        query += joining_string

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
    except psycopg2.Error as error:
        connection.rollback()
        connection.close()
        logging.exception(error)
        return None

    logging.info(COMPLETE_OPERATION_MESSAGE.format(operation='select'))
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

    if distinct:
        distinct_string: Composed | SQL
        distinct_string = sql.SQL(' DISTINCT ON ({table}.{field})').format(
            table=sql.Identifier(distinct[0]),
            field=sql.Identifier(distinct[1]))
    else:
        distinct_string = sql.SQL('')

    query = sql.SQL(string_pattern).format(
        table=sql.Identifier(table),
        distinct=distinct_string,
        fields=selection_fields)

    return query


def _generate_joining_string(table: str,
                             joining: tuple[tuple[str, str], str],
                             ) -> Composed:
    """Generate a SQL LEFT JOIN string."""
    string_pattern = 'LEFT JOIN {secondary_table} ON {main_table}.{main_field}'\
                     ' = {secondary_table}.{secondary_field}\n'
    main_table = sql.Identifier(table)
    main_field = sql.Identifier(joining[1])
    secondary_table = sql.Identifier(joining[0][0])
    secondary_field = sql.Identifier(joining[0][1])

    joining_string = sql.SQL(string_pattern).format(
        main_table=main_table,
        main_field=main_field,
        secondary_table=secondary_table,
        secondary_field=secondary_field)

    return joining_string


def _generate_filtering_string(filtering: tuple[tuple[str, str], Any],
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
