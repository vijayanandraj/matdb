import logging
from matdb.core import Database

logging.basicConfig(level=logging.DEBUG)
logging.debug("Welcome")
db_string = "mssql://sqltest:vijay@localhost:1433/aryan_db?min_size=5&max_size=20"
database = Database(db_string)
database.connect()
print("Connection established")
rows = database.fetch_all(query="select * from emp")
print(rows)

