import logging
from matdb.core import Database

logging.basicConfig(level=logging.DEBUG)
logging.debug("Welcome")
db_string = "mysql://vijay:Vinisha329$@localhost:3306/test"
database = Database(db_string)
database.connect()
print("Connection established....")
rows = database.fetch_all(query="select * from brands")
print(rows)

query = "INSERT INTO emp(emp_id, emp_name) VALUES (:emp_id, :emp_name)"
values = {"emp_id":933291, "emp_name":"AVijay"}
database.execute(query=query, values=values)