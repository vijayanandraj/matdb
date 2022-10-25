import typing
from collections.abc import Sequence

from sqlalchemy.sql import ClauseElement


class DatabaseBackend:
    def connect(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    def disconnect(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    def connection(self) -> "ConnectionBackend":
        raise NotImplementedError()  # pragma: no cover


class ConnectionBackend:
    def acquire(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    def release(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    def fetch_all(self, query: ClauseElement) -> typing.List["Record"]:
        raise NotImplementedError()  # pragma: no cover

    def fetch_all_as_json_string(self, query: ClauseElement) -> typing.AnyStr:
        raise NotImplementedError()  # pragma: no cover

    def fetch_one(self, query: ClauseElement) -> typing.Optional["Record"]:
        raise NotImplementedError()  # pragma: no cover

    def fetch_val(
        self, query: ClauseElement, column: typing.Any = 0
    ) -> typing.Any:
        row = self.fetch_one(query)
        return None if row is None else row[column]

    def execute(self, query: ClauseElement) -> typing.Any:
        raise NotImplementedError()  # pragma: no cover

    def execute_many(self, queries: typing.List[ClauseElement]) -> None:
        raise NotImplementedError()  # pragma: no cover

    def iterate(
        self, query: ClauseElement
    ) -> typing.AsyncGenerator[typing.Mapping, None]:
        raise NotImplementedError()  # pragma: no cover
        yield True  # pragma: no cover

    def transaction(self) -> "TransactionBackend":
        raise NotImplementedError()  # pragma: no cover

    @property
    def raw_connection(self) -> typing.Any:
        raise NotImplementedError()  # pragma: no cover


class TransactionBackend:
    def start(
        self, is_root: bool, extra_options: typing.Dict[typing.Any, typing.Any]
    ) -> None:
        raise NotImplementedError()  # pragma: no cover

    def commit(self) -> None:
        raise NotImplementedError()  # pragma: no cover

    def rollback(self) -> None:
        raise NotImplementedError()  # pragma: no cover


class Record(Sequence):
    @property
    def _mapping(self) -> typing.Mapping:
        raise NotImplementedError()  # pragma: no cover

    def __getitem__(self, key: typing.Any) -> typing.Any:
        raise NotImplementedError()  # pragma: no cover