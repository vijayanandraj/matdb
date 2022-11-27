import logging
from matdb.core import Database
import random

logging.basicConfig(level=logging.DEBUG)
logging.debug("Welcome")
db_string = "mssql://sqltest:vijay@localhost:1433/aryan_db?min_size=5&max_size=20"
database = Database(db_string)
database.connect()
print("Connection established")

def trans_test(database):
    print("Transaction Test...")
    print(database.is_connected)
    try:
        with database.connection() as connection:
            with connection.transaction():
                query = "INSERT INTO emp(emp_id, emp_name) VALUES (:emp_id, :emp_name)"
                empid = random.randrange(1000, 10000)
                print(f"Emp ID ==> {empid}")
                values = {"emp_id": empid, "emp_name": "Vijay"}
                rowcnt = database.execute(query=query, values=values)
                print(f"Row count == > {rowcnt}")
                query1 = "update dept set dep_name = :dept_name where deptid = :deptid"
                values1 = {"dept_name" : "New Marketing", "deptid" : 10}
                rnt1 = database.execute(query=query1, values = values1)
    except Exception as e:
        logging.info(str(e))
        raise
    finally:
        logging.info("In finally block")


def db_test(database):
    print(database.is_connected)

if __name__ == '__main__':
    trans_test(database)
    db_test(database)