import logging
from matdb import Database
import json
import timeit
import random

import decimal, datetime


logging.basicConfig(level=logging.DEBUG)
logging.debug("Welcome")
db_string = "mysql://vijay:Vinisha329$@localhost:3306/test?min_size=5&max_size=20&pre_create_num=5"
#db_string = "mysql://vijay:Vinisha329$@localhost:3306/test"
database = Database(db_string)
database.connect()
print("Connection established....")
# rows = database.fetch_all(query="select * from brands")
# print(rows)
# print(json.dumps([dict(r) for r in rows], default=alchemyencoder))
def mysql_read():
    json_str = database.fetch_all_as_json_string(query="select * from brands")
    print(json_str)
    return json_str

def mysql_write():
    query = "INSERT INTO emp(emp_id, emp_name) VALUES (:emp_id, :emp_name)"
    # values = {}
    empid = random.randrange(1000, 10000)
    values = {"emp_id": empid, "emp_name": "Vijay"}
    rowcnt = database.execute(query=query, values=values)
    print(f"Row count == > {rowcnt}")


#print(json_str)

if __name__ == '__main__':
    print(timeit.Timer(mysql_write).timeit(number=500))