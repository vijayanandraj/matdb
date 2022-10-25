
import logging
import typing
import uuid
import json
import datetime, decimal
import pymysql
from sqlalchemy.dialects import *
from sqlalchemy.engine.cursor import CursorResultMetaData
from sqlalchemy.engine.interfaces import Dialect, ExecutionContext
from sqlalchemy.engine.row import Row
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.ddl import DDLElement

from matdb.core import LOG_EXTRA, DatabaseURL
from matdb.interfaces import (
    ConnectionBackend,
    DatabaseBackend,
    Record,
    TransactionBackend,
)

logger = logging.getLogger("databases")


class MySQLBackend(DatabaseBackend):
    def __init__(
        self, database_url: typing.Union[DatabaseURL, str], **options: typing.Any
    ) -> None:
        self._database_url = DatabaseURL(database_url)
        self._options = options
        self._dialect = mysql.pymysql.dialect(paramstyle="pyformat")
        # aiosqlite does not support decimals
        self._dialect.supports_native_decimal = True
        self._pool = MySQLPool(self._database_url, **self._options)

    def connect(self) -> None:
        pass
        # assert self._pool is None, "DatabaseBackend is already running"
        # self._pool = await aiomysql.create_pool(
        #     host=self._database_url.hostname,
        #     port=self._database_url.port or 3306,
        #     user=self._database_url.username or getpass.getuser(),
        #     password=self._database_url.password,
        #     db=self._database_url.database,
        #     autocommit=True,
        # )

    def disconnect(self) -> None:
        pass
        # assert self._pool is not None, "DatabaseBackend is not running"
        # self._pool.close()
        # await self._pool.wait_closed()
        # self._pool = None

    def connection(self) -> "MySQLConnection":
        return MySQLConnection(self._pool, self._dialect)

# Modify this to
class MySQLPool:
    def __init__(self, url: DatabaseURL, **options: typing.Any) -> None:
        self._url = url
        self._options = options

    def acquire(self) -> pymysql.Connection:
        host=self._url.hostname
        port=self._url.port or 3306
        user=self._url.username
        password=self._url.password
        db=self._url.database
        connection = pymysql.connect(host=host, port=port, user=user, password=password, db=db, autocommit=True)
        return connection

    def release(self, connection: pymysql.Connection) -> None:
        connection.close()

def alchemyencoder(obj):
    """JSON encoder function for SQLAlchemy special classes."""
    if isinstance(obj, datetime.date):
        return obj.isoformat()
    elif isinstance(obj, decimal.Decimal):
        return float(obj)

class CompilationContext:
    def __init__(self, context: ExecutionContext):
        self.context = context


class MySQLConnection(ConnectionBackend):
    def __init__(self, pool: MySQLPool, dialect: Dialect):
        self._pool = pool
        self._dialect = dialect
        self._connection = None  # type: typing.Optional[pymysql.Connection]

    def acquire(self) -> None:
        assert self._connection is None, "Connection is already acquired"
        self._connection = self._pool.acquire()

    def release(self) -> None:
        assert self._connection is not None, "Connection is not acquired"
        self._pool.release(self._connection)
        self._connection = None

    def fetch_all(self, query: ClauseElement) -> typing.List[Record]:
        assert self._connection is not None, "Connection is not acquired"
        query_str, args, context = self._compile(query)
        cursor = self._connection.cursor()
        try:
            cursor.execute(query_str, args)
            rows = cursor.fetchall()
            metadata = CursorResultMetaData(context, cursor.description)
            return [
                Row(
                    metadata,
                    metadata._processors,
                    metadata._keymap,
                    Row._default_key_style,
                    row,
                )
                for row in rows
            ]
        finally:
            cursor.close()

    def fetch_all_as_json_string(self, query: ClauseElement) -> typing.AnyStr:
        rows = self.fetch_all(query)
        result = json.dumps([dict(r) for r in rows], default=alchemyencoder)
        return result


    def fetch_one(self, query: ClauseElement) -> typing.Optional[Record]:
        assert self._connection is not None, "Connection is not acquired"
        query_str, args, context = self._compile(query)
        cursor = self._connection.cursor()
        try:
            cursor.execute(query_str, args)
            row = cursor.fetchone()
            if row is None:
                return None
            metadata = CursorResultMetaData(context, cursor.description)
            return Row(
                metadata,
                metadata._processors,
                metadata._keymap,
                Row._default_key_style,
                row,
            )
        finally:
            cursor.close()

    def execute(self, query: ClauseElement) -> typing.Any:
        assert self._connection is not None, "Connection is not acquired"
        query_str, args, context = self._compile(query)
        cursor = self._connection.cursor()
        try:
            cursor.execute(query_str, args)
            if cursor.lastrowid == 0:
                return cursor.rowcount
            return cursor.lastrowid
        finally:
            cursor.close()

    def execute_many(self, queries: typing.List[ClauseElement]) -> None:
        assert self._connection is not None, "Connection is not acquired"
        cursor = self._connection.cursor()
        try:
            for single_query in queries:
                single_query, args, context = self._compile(single_query)
                cursor.execute(single_query, args)
        finally:
            cursor.close()

    def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[typing.Any, None]:
        assert self._connection is not None, "Connection is not acquired"
        query_str, args, context = self._compile(query)
        cursor = self._connection.cursor()
        try:
            cursor.execute(query_str, args)
            metadata = CursorResultMetaData(context, cursor.description)
            for row in cursor:
                yield Row(
                    metadata,
                    metadata._processors,
                    metadata._keymap,
                    Row._default_key_style,
                    row,
                )
        finally:
            cursor.close()

    def transaction(self) -> TransactionBackend:
        return MySQLTransaction(self)

    def _compile(
        self, query: ClauseElement
    ) -> typing.Tuple[str, dict, CompilationContext]:
        compiled = query.compile(
            dialect=self._dialect, compile_kwargs={"render_postcompile": True}
        )

        execution_context = self._dialect.execution_ctx_cls()
        execution_context.dialect = self._dialect

        if not isinstance(query, DDLElement):
            args = compiled.construct_params()
            for key, val in args.items():
                if key in compiled._bind_processors:
                    args[key] = compiled._bind_processors[key](val)

            execution_context.result_column_struct = (
                compiled._result_columns,
                compiled._ordered_columns,
                compiled._textual_ordered_columns,
                compiled._loose_column_name_matching,
            )
        else:
            args = {}

        query_message = compiled.string.replace(" \n", " ").replace("\n", " ")
        logger.debug("Query: %s Args: %s", query_message, repr(args), extra=LOG_EXTRA)
        return compiled.string, args, CompilationContext(execution_context)


    @property
    def raw_connection(self) -> pymysql.Connection:
        assert self._connection is not None, "Connection is not acquired"
        return self._connection


class MySQLTransaction(TransactionBackend):
    def __init__(self, connection: MySQLConnection):
        self._connection = connection
        self._is_root = False
        self._savepoint_name = ""

    def start(
        self, is_root: bool, extra_options: typing.Dict[typing.Any, typing.Any]
    ) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        self._is_root = is_root
        if self._is_root:
            self._connection._connection.begin()
        else:
            id = str(uuid.uuid4()).replace("-", "_")
            self._savepoint_name = f"STARLETTE_SAVEPOINT_{id}"
            cursor = self._connection._connection.cursor()
            try:
                cursor.execute(f"SAVEPOINT {self._savepoint_name}")
            finally:
                cursor.close()

    def commit(self) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        if self._is_root:
            self._connection._connection.commit()
        else:
            cursor = self._connection._connection.cursor()
            try:
                cursor.execute(f"RELEASE SAVEPOINT {self._savepoint_name}")
            finally:
                cursor.close()

    def rollback(self) -> None:
        assert self._connection._connection is not None, "Connection is not acquired"
        if self._is_root:
            self._connection._connection.rollback()
        else:
            cursor = self._connection._connection.cursor()
            try:
                cursor.execute(f"ROLLBACK TO SAVEPOINT {self._savepoint_name}")
            finally:
                cursor.close()

