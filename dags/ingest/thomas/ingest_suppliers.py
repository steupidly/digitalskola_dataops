import psycopg2
import csv
import sys
from airflow.models import Variable

psql_host = Variable.get("psql_host")
psql_port = int(Variable.get("psql_port"))
psql_user = Variable.get("psql_user")
psql_password = Variable.get("psql_password")
psql_db = Variable.get("psql_db")

conn = None
sql = """select * from suppliers"""
csv_file_path = '/root/output/thomas/suppliers/suppliers.csv'

try:
    #connection to PostgreSQL
    conn = psycopg2.connect(
        user=psql_user,
        password=psql_password,
        dbname=psql_db,
        host=psql_host,
        port=psql_port
    )

    #run PostgreSQL query
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
finally:
    if conn:
        conn.close()

# Continue only if there are rows returned.
if rows:
    # New empty list called 'result'. This will be written to a file.
    result = list()

    # The row name is the first entry for each entity in the description tuple.
    column_names = list()
    for i in cursor.description:
        column_names.append(i[0])

    result.append(column_names)
    for row in rows:
        result.append(row)

    # Write result to file.
    with open(csv_file_path, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in result:
            csvwriter.writerow(row)
else:
    print("No rows found for query: {}".format(sql))
    
