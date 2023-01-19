

import json
import pyodbc
import os
from table_data.Column import Column
from table_data.Table import Table

f = open('generator.json', "r")
configuration = json.load(f)

print(configuration)
database_name = configuration["database"]

schema_name = configuration["schema"]

print("[INFO] Loading Table Schema")

selected_tables = configuration["select_table_names"]

dbt_file_path = configuration["dbt_file_path"]

#initialize the list of tables in our schema
table_list = []

# Connect to the Databricks cluster
conn = pyodbc.connect("DSN={0}".format(configuration["odbc_datasource"]), autocommit=True)

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



print("[INFO] Table Schema Loaded")

while True:
    key_press = input("Press the number next to the action.\n"
                      "1)List Tables"
                      "\n2)Write standard DBT models for all tables"
                      "\n3)Write Schema Models for all tables"
                      "\n4)Quit\n")
    if key_press == "4":
        exit(0)
    if key_press == "1":
        for table_ in table_list:
            print("{0}.{1}".format(configuration["schema"], table_.name))
        print("Total {0} tables".format(len(table_list)))
    if key_press == "2":
        print("[INFO] Writing models for all tables")
        folder_name = input("Enter name for the list of models")
        model_path = dbt_file_path + "/{0}/models/{1}".format(configuration["dbt_data"], folder_name)
        # check if folder exists , if so do nothing , else make a directory
        if os.path.exists(model_path):
            print("[WARNING] Directory exists , please delete and enter a new one")
        else:
            print("[INFO] Creating directory")
            os.mkdir(model_path)
            for table in table_list:
                table.write_model_sql(model_path)

            print("[INFO] Models created. please rerun DBT to load them")
    if key_press == "3":
        print("[INFO] Writing models for all tables")
        folder_name = input("Enter name for the list of models")
        model_path = dbt_file_path + "/{0}/models/{1}".format(configuration["dbt_data"], folder_name)
        # check if folder exists , if so do nothing , else make a directory
        if os.path.exists(model_path):
            print("[WARNING] Directory exists , please delete and enter a new one")
        else:
            print("[INFO] Creating directory")
            os.mkdir(model_path)
            #write schema yaml

            table_yaml_list = []
            for table in table_list:
                table_yaml_list.append(table.get_yaml_dict())

            table.generate_schema(table_yaml_list, model_path)

            for table in table_list:
                table.write_model_sql_with_schema(model_path)

            print("[INFO] Models created. please rerun DBT to load them")





"""
add a help flag
"""

