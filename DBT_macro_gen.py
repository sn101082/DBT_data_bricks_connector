import pyodbc

data_base = "Azuredatabricks"
odbc_source_name = "test_sandbox"
schema = "test_demo"
table_name = "metadata"

conn = pyodbc.connect("DSN={0}".format(odbc_source_name), autocommit=True)

table_sql = f"select * from {schema}.{table_name}"
data_cursor = conn.cursor()
data_cursor.execute(table_sql)

macro_name = input("Please enter the macro function name ")
print("[INFO] {0} is the macro name".format(macro_name))

folder_location = input("Please enter the macro folder location ")
print("[INFO] {0} is the macro folder location".format(macro_name))

source_table = ""
source_key_columns = []
target_table = ""
target_key_columns = []
output_columns = []
join_type = ""
# source_name,key columns,target_table,target_keycolumn,output columns, jointype
for col_row in data_cursor.fetchall():
    # print(f"{col_row[0]},{col_row[1]},{col_row[2]},{col_row[3]},{col_row[4]},{col_row[5]}")
    if col_row[0] is not None:
        source_table = col_row[0]
    if col_row[1] is not None:
        source_key_columns.append(col_row[1])
    if col_row[2] is not None:
        target_table = col_row[2]
    if col_row[3] is not None:
        target_key_columns.append(col_row[3])
    if col_row[4] is not None:
        output_columns.append(col_row[4])
    if col_row[5] is not None:
        join_type = col_row[5]


def generate_select_string(t_table, t_cols):
    sel_string = "select \n"
    for t_col in t_cols:
        sel_string = sel_string + f"{t_table}.{t_col}\n"
    sel_string = sel_string + f" from {t_table}"
    return sel_string


def generate_join_indexes(t_table, t_cols, s_table, s_cols):
    sel_string = ""
    index_list = []
    for t_col, s_col in zip(t_cols, s_cols):
        index_list.append(f"{t_table}.{t_col} = {s_table}.{s_col}")
    if len(index_list) == 1:
        return index_list[0]
    elif len(index_list) > 1:
        return " and ".join(index_list)


macro_text = f"""
{{% macro {macro_name}() %}}
{{{{
 config(
    materialized = 'table'
    )
}}}}
{generate_select_string(target_table, output_columns)}
{join_type}
{source_table}
ON  {generate_join_indexes(target_table, target_key_columns, source_table, source_key_columns)}

{{% endmacro %}}
}}
"""

sql_model_file = open(f"{folder_location}/{macro_name}.sql", "w")
sql_model_file.write(macro_text)
sql_model_file.close()


print(macro_name)

print(f"[INFO] {macro_name}.sql File written")