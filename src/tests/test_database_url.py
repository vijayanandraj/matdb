from urllib.parse import quote

import pytest

from matdb.core import DatabaseURL

def test_database_url_repr():
    u = DatabaseURL("mysql://localhost/name")
    assert repr(u) == "DatabaseURL('mysql://localhost/name')"

    u = DatabaseURL("mysql://username@localhost/name")
    assert repr(u) == "DatabaseURL('mysql://username@localhost/name')"

    u = DatabaseURL("mysql://username:password@localhost/name")
    assert repr(u) == "DatabaseURL('mysql://username:********@localhost/name')"

    u = DatabaseURL(f"mysql://username:{quote('[password')}@localhost/name")
    assert repr(u) == "DatabaseURL('mysql://username:********@localhost/name')"


def test_mysql_database_url_properties():
    u = DatabaseURL("mysql://username:password@localhost:123/mydatabase")
    assert u.dialect == "mysql"
    assert u.username == "username"
    assert u.password == "password"
    assert u.hostname == "localhost"
    assert u.port == 123
    assert u.database == "mydatabase"

