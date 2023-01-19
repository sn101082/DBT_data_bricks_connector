"""
This script connects to the datasource and provides a csv representation of each table and column
these columns can be transformed. The output would be in a user defined  folder.
The user defined folder is the input for the table_model_generator. which will generate the required
tables model files.

"""

import json
import pyodbc
import os
from table_data.Column import Column
from table_data.Table import Table

config_dict = {"database": "", "schema": "", "odbc_datasource": "", "dbt_file_path": ""}

db_name = input("Please enter Database Name ")
config_dict["database"] = db_name
print("[INFO] {0} is the Database Name".format(db_name))


schema = input("Please enter Schema Name ")
print("[INFO] {0} is the Schema Name".format(schema))
config_dict["schema"] = schema

odbc_datasource = input("Please enter ODBC Datasource Name ")
print("[INFO] {0} is the ODBC Datasource Name".format(odbc_datasource))
config_dict["odbc_datasource"] = odbc_datasource


dbt_file_path = input("Please enter DBT File Path ")
print("[INFO] {0} is the DBT File Path".format(dbt_file_path))
config_dict["dbt_file_path"] = dbt_file_path


generator_directory = input("Please enter Generator  File Path ")

while os.path.exists(generator_directory):
    print("[Error] Path already Exists")
    generator_directory = input("Please enter Generator  File Path ")

print("[INFO] {0} is the Generator File Path".format(generator_directory))


print("[INFO] Making Temp Folder ___temp")
print("[INFO] Getting Schema Information")

os.mkdir(generator_directory)


database_name = db_name

schema_name = schema

print("[INFO] Loading Table Schema")

selected_tables = []


# initialize the list of tables in our schema
table_list = []

# Connect to the Databricks cluster
conn = pyodbc.connect("DSN={0}".format(odbc_datasource), autocommit=True)

print("[INFO] Connected")

# get list of tables.
cursor = conn.cursor()
cursor.execute(f"SHOW TABLES in  {schema_name}")

column_cursor = conn.cursor()

for row in cursor.fetchall():
    is_temp = row[2]
    table_name = row[1]
    if len(selected_tables) > 0:
        if table_name in selected_tables:
            table_sql = f"DESCRIBE TABLE {table_name}"
            column_cursor.execute(table_sql)
            column_list = []
            count = 1
            for col_row in column_cursor.fetchall():
                col = Column(col_row[0], count, col_row[1], None)
                column_list.append(col)
            table = Table(table_name, schema_name, database_name, column_list, is_temp)
            table_list.append(table)

    else:
        table_sql = f"DESCRIBE TABLE {table_name}"
        column_cursor.execute(table_sql)
        column_list = []
        count = 1
        for col_row in column_cursor.fetchall():
            col = Column(col_row[0], count, col_row[1], None)
            column_list.append(col)
        table = Table(table_name, schema_name, database_name, column_list, is_temp)
        table_list.append(table)


print("[INFO] Tables Loaded ")

all_tables = input("Do you want to output all tables? (y/N) ")

if all_tables == "y":
    print("[INFO] Writing tables to file")

    for tab in table_list:
        tab.generate_csv_file(generator_directory)
else:
    for tab in table_list:
        print(tab.name)

tables = input("Please enter a list of tables in commas separated values?: ")

tab_list = tables.split(",")

for tab in table_list:
    if tab.name in tab_list:
        print("[INFO] Writing {0} Table".format(tab.name))
        tab.generate_csv_file(generator_directory)

print("[INFO] Writing config ")
with open('{0}/config.json'.format(generator_directory), 'w') as outfile:
    json.dump(config_dict, outfile)
