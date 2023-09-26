import datetime
import logging
import os
from typing import Any

from dotenv import load_dotenv
from psycopg2 import DatabaseError, Error, connect, sql
from psycopg2.extras import RealDictCursor

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')

OPEN_CONNECTION_MESSAGE = 'A connection to the database has been established'
CLOSE_CONNECTION_MESSAGE = 'The changes are committed and '\
                           'the connection to the database is close'
COMPLETE_OPERATION_MESSAGE = 'The {operation} operation is completed'


def open_connection():
    conn = None

    try:
        conn = connect(DATABASE_URL, cursor_factory=RealDictCursor)
    except DatabaseError as error:
        logging.exception(error)

    logging.info(OPEN_CONNECTION_MESSAGE)
    return conn


def insert_data(connection,
                table: str,
                fields: list[str],
                data: dict[str, Any],
                ) -> bool:

    fields = [*fields, 'created_at']
    created_at = datetime.datetime.now()
    data.update(created_at=created_at)

    query = sql.SQL("INSERT INTO {table} ({fields}) "
                    "VALUES (%(name)s, %(created_at)s);").format(
                        table=sql.Identifier(table),
                        fields=sql.SQL(',').join(map(sql.Identifier, fields)))

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


def select_data(connection,
                table: str,
                fields: list[str],
                filter_: tuple[str, str] | None = None,
                sorting: tuple[str, str] | None = None,
                ) -> list[dict] | None:

    query = sql.SQL("SELECT {fields} FROM {table}\n").format(
        table=sql.Identifier(table),
        fields=sql.SQL(',').join(map(sql.Identifier, fields)),)
    query_end = sql.SQL(';')

    if filter_ is not None:
        where = sql.SQL('WHERE {field} = {id}\n').format(
            field=sql.Identifier(filter_[0]),
            id=sql.Literal(filter_[1]))
        query = query + where

    if sorting is not None:
        sort = sql.SQL('ORDER BY {field} {order}').format(
            field=sql.Identifier(sorting[0]),
            order=sql.SQL(sorting[1]))
        query = query + sort

    result_query = query + query_end

    try:
        with connection.cursor() as cursor:
            cursor.execute(result_query)
            data = cursor.fetchall()
    except Error as error:
        connection.rollback()
        connection.close()
        logging.exception(error)
        return None

    logging.info(COMPLETE_OPERATION_MESSAGE.format(operation='select'))
    return data


def commit_changes(connection) -> None:
    connection.commit()
    connection.close()
    logging.info(CLOSE_CONNECTION_MESSAGE)
