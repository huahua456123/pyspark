import cx_Oracle
username = "scott"
userpwd = "tiger"
host = "127.0.0.1"
port = 1521
dbname = "orcl"
dsn=cx_Oracle.makedsn(host, port, dbname)
connection=cx_Oracle.connect(username, userpwd, dsn)
cursor = connection.cursor()
sql = "select * from tab"
cursor.execute(sql)
result = cursor.fetchall()
count = cursor.rowcount
print("=====================" )
print("Total:", count)
print("=====================")
for row in result:
    print (row)
cursor.close()
connection.close()
