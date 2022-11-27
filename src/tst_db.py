import logging
from matdb.core import Database

logging.basicConfig(level=logging.DEBUG)
logging.debug("Welcome")
#db_string = "mysql://vijay:Vinisha329$@localhost:3306/test?min_size=5&max_size=20&pre_create_num=5"
db_string = "mysql://vijay:Vinisha329$@localhost:3306/test?min_size=5&max_size=20"
database = Database(db_string)
database.connect()
print("Connection established....")
rows = database.fetch_all(query="select * from brands")
print(rows)

with database.connection() as connection:
    with connection.transaction():
        query = "INSERT INTO emp(emp_id, emp_name) VALUES (:emp_id, :emp_name)"
        values = {"emp_id":93391, "emp_name":"AVijay"}
        database.execute(query=query, values=values)