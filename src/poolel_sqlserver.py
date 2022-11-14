import pymssql
from dbutils.pooled_db import PooledDB
conn_kwargs = { 'host' : 'localhost', 'user' : 'sqltest', 'password' : 'vijay', 'database' : 'aryan_db'}
pool = PooledDB(pymssql, mincached=5, maxcached=10, maxconnections=10,**conn_kwargs)
db = pool.connection()
cursor = db.cursor()
cursor.execute('select * from emp')
print(cursor.fetchall())