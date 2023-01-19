import json
import pyodbc
import os
from table_data.Column import Column
from table_data.Table import Table

input_folder_path = input("Please enter input generator folder path: ")
print("[INFO] {0} is the input folder path".format(input_folder_path))

print("[INFO} Loading data")

f = open('{0}/config.json'.format(input_folder_path), "r")
configuration = json.load(f)
print(configuration)

model_folder = input("Please model folder name: ")
print("[INFO] {0} is the model folder".format(input_folder_path))

model_path =  "{0}/models/{1}".format(configuration["dbt_file_path"], model_folder)
# check if folder exists , if so do nothing , else make a directory
while  os.path.exists(model_path):
    print("[WARNING] Directory exists , please delete or  enter a new one")
    model_folder = input("Please model folder name")
    print("[INFO] {0} is the model folder".format(input_folder_path))
    model_path = "{0}/models/{1}".format(configuration["dbt_file_path"], model_folder)

print("[INFO] Creating directory")
os.mkdir(model_path)
files_list = os.listdir("{0}".format(input_folder_path))

table_list = []
for files in files_list:
    if ".csv" in files:
        table_list.append(Table.from_csv(configuration["database"], configuration["schema"],
                                         "{0}/{1}".format(input_folder_path, files)))

print("[INFO] Writing models into {0}".format(model_path))

print("{0} Tables loaded".format(len(table_list)))

for tab in table_list:
    tab.write_model_transform_sql(model_path)

print("[INFO] Models created. please rerun DBT to load them")