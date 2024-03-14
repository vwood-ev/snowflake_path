#!/usr/bin/env python


import snowflake.connector

USER = os.getenv('SNOWSQL_USER')
PASSWORD = os.getenv('SNOWSQL_PWD')
ACCOUNT = os.getenv('SNOWSQL_ACCOUNT')


conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
)


# Change session parameters
# conn.cursor().execute("ALTER SESSION SET QUERY_TAG = 'EndOfMonthFinancials'")

#
# Creating DB objects
#
conn.cursor().execute("CREATE WAREHOUSE IF NOT EXISTS tiny_warehouse_mg")

# Create warehouse automatically sets it as the active warehouse
# conn.cursor().execute("USE WAREHOUSE tiny_warehouse_mg")

conn.cursor().execute("CREATE DATABASE IF NOT EXISTS testdb")

# Again with creating the DB
# conn.cursor().execute("USE DATABASE testdb")

conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS testschema")

# Once again, create schema sets it as active
# conn.cursor().execute("USE SCHEMA testschema")
# conn.cursor().execute("USE SCHEMA otherdb.testschema")

conn.cursor().execute(
    "CREATE OR REPLACE TABLE "
    "test_table(col1 integer, col2 string)")

#
# Insert Data
#
# From commands
conn.cursor().execute(
    "INSERT INTO test_table(col1, col2) "
    "VALUES(123, 'test string1'),(456, 'test string2')")

# From files
conn.cursor().execute("PUT file:///tmp/data/file* @%test_table")
conn.cursor().execute("COPY INTO test_table")

# With variable bindings
con.cursor().execute(
    "INSERT INTO testtable(col1, col2) "
    "VALUES(%s, %s)", (
        789,
        'test string3'
    ))

#
# Query Data
#

# print items
col1, col2 = conn.cursor().execute("SELECT col1, col2 FROM test_table").fetchone()
print('{0}, {1}'.format(col1, col2))


# Print columns
for (col1, col2) in conn.cursor().execute("SELECT col1, col2 FROM test_table"):
	print('{0}, {1}'.format(col1, col2))


# conn.close()

