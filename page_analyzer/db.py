import datetime
import logging
import os
from typing import Any

from dotenv import load_dotenv
from psycopg2 import DatabaseError, Error, connect, sql
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor, RealDictRow
from psycopg2.sql import SQL, Composed

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')

OPEN_CONNECTION_MESSAGE = 'A connection to the database has been established'
CLOSE_CONNECTION_MESSAGE = 'The changes are committed and '\
                           'the connection to the database is close'
COMPLETE_OPERATION_MESSAGE = 'The {operation} operation is completed'


def open_connection() -> connection | None:
    conn = None

    try:
        conn = connect(DATABASE_URL, cursor_factory=RealDictCursor)
    except DatabaseError as error:
        logging.exception(error)

    logging.info(OPEN_CONNECTION_MESSAGE)
    return conn


def insert_data(connection: connection,
                table: str,
                fields: list[str],
                data: dict[str, Any],
                ) -> bool:

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
    except Error as error:
        connection.rollback()
        connection.close()
        logging.exception(error)
        return False

    logging.info(COMPLETE_OPERATION_MESSAGE.format(operation='insert'))
    return True


def _get_selection_string(table: str,
                          fields: list[tuple[str, str]],
                          distinct: tuple[str, str] | None = None,
                          joining: tuple[tuple[str, str], str] | None = None,
                          ) -> Composed:

    selection_string = 'SELECT{distinct} {fields} FROM {table}\n'
    selection_fields = sql.SQL(',').join(
        sql.Identifier(table) + sql.SQL('.') + sql.Identifier(field)
        for table, field in fields)

    if distinct:
        distinct_string: SQL | Composed
        distinct_string = sql.SQL(' DISTINCT ON ({table}.{field})').format(
            table=sql.Identifier(distinct[0]),
            field=sql.Identifier(distinct[1]))
    else:
        distinct_string = sql.SQL('')

    query = sql.SQL(selection_string).format(
        table=sql.Identifier(table),
        distinct=distinct_string,
        fields=selection_fields)

    if joining:
        join_string = 'LEFT JOIN {joining_table} ON {main_table}.{main_field} '\
                      '= {joining_table}.{joining_field}\n'
        main_table = sql.Identifier(table)
        main_field = sql.Identifier(joining[1])
        joining_table = sql.Identifier(joining[0][0])
        joining_field = sql.Identifier(joining[0][1])
        query = query + sql.SQL(join_string).format(
            main_table=main_table,
            main_field=main_field,
            joining_table=joining_table,
            joining_field=joining_field)
    return query


def select_data(connection: connection,
                table: str,
                fields: list[tuple[str, str]],
                distinct: tuple[str, str] | None = None,
                joining: tuple[tuple[str, str], str] | None = None,
                filtering: tuple[tuple[str, str], Any] | None = None,
                sorting: list[tuple[tuple[str, str], str]] | None = None,
                ) -> list[RealDictRow] | None:

    query = _get_selection_string(table=table,
                                  fields=fields,
                                  distinct=distinct,
                                  joining=joining)
    query_end = sql.SQL(';')

    if filtering is not None:
        filtering_table = sql.Identifier(filtering[0][0])
        filtering_field = sql.Identifier(filtering[0][1])
        entry_id = sql.Literal(filtering[1])
        where = sql.SQL('WHERE {table}.{field} = {id}\n').format(
            table=filtering_table,
            field=filtering_field,
            id=entry_id)
        query += where

    # I wanted to implement through generating expressions
    # but I couldn't shorten the string
    if sorting is not None:
        temp = '{table}.{field} {order}'
        sorting_fields = []
        for field, order in sorting:
            sorting_fields.append(sql.SQL(temp).format(
                table=sql.Identifier(field[0]),
                field=sql.Identifier(field[1]),
                order=sql.SQL(order)
            ))

        sort = sql.SQL('ORDER BY {sorting_fields}').format(
            sorting_fields=sql.SQL(',').join(sorting_fields))
        query += sort

    result_query = query + query_end
    print(result_query.as_string(connection))
    try:
        with connection.cursor() as cursor:
            cursor.execute(result_query)
            data: list[RealDictRow] = cursor.fetchall()  # type: ignore
    except Error as error:
        connection.rollback()
        connection.close()
        logging.exception(error)
        return None

    logging.info(COMPLETE_OPERATION_MESSAGE.format(operation='select'))
    return data


def commit_changes(connection: connection) -> None:
    connection.commit()
    connection.close()
    logging.info(CLOSE_CONNECTION_MESSAGE)
