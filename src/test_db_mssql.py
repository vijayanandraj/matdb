import logging
from matdb.core import Database

logging.basicConfig(level=logging.DEBUG)
logging.debug("Welcome")
db_string = "mssql://sqltest:vijay@localhost:1433/aryan_db"
database = Database(db_string)
database.connect()
print("Connection established")
rows = database.fetch_all(query="select * from emp")
print(rows)