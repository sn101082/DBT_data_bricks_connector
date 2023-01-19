from table_data import Column
import yaml
import pandas as pd

class Table:

    def __init__(self, name, schema, database, column_list, is_temp):
        self.name = name
        self.schema = schema
        self.database = database
        self.column_list = column_list
        self.is_temp = is_temp

    @classmethod
    def from_data_frame(cls, df):

        # import the excel data frame to fill the table
        # should contain the table specific columns only
        column_list = []
        table_name = ""
        schema = ""
        database = ""

        for row in df.itertuples():
            print(row)
            table_name = row.Src_Table
            schema = row.schema

            column_list.append(Column.Column(row.Src_Column,
                                             row.index, None,
                                             None, row.Transformation,
                                             row.Target_col))
        return cls(table_name, schema, database, column_list, False)


    @classmethod
    def from_csv(cls, database, schema, file_name):
        df = pd.read_csv(file_name)
        # import the excel data frame to fill the table
        # should contain the table specific columns only
        column_list = []
        table_name = ""

        for row in df.itertuples():
            table_name = row.table_name
            column_list.append(Column.Column(row.column,
                                             row.index, None,
                                             None, row.transform,
                                             row.target_column))
        return cls(table_name, schema, database, column_list, False)


    def generate_model_sql(self):
        model_sql = "with {0}_model as (\n select \n".format(self.name)
        for column in self.column_list:
            model_sql = model_sql + "{0},".format(column.name)
        model_sql = model_sql + "extend1,"
        model_sql = model_sql + "extend2,"
        model_sql = model_sql + "extend3,"
        model_sql = model_sql + "extend4,"
        model_sql = model_sql + "extend5"

        model_sql = model_sql + "\n from {0} )".format(self.name)
        model_sql = model_sql + "\n\n select * from {0}.{1}_model".format(self.schema, self.name)
        return model_sql

    def generate_model_transform_sql(self):
        model_sql = "with {0}_model as (\n select \n".format(self.name)
        for column in self.column_list:
            model_sql = model_sql + "{0},\n".format(column.get_column_sql())
        model_sql = model_sql[:-2] + "\n from {0}.{1} )".format(self.schema, self.name)
        model_sql = model_sql + "\n\n select * from  {1}_model".format(self.schema, self.name)
        return model_sql

    def write_model_transform_sql(self, path):
        sql_model_file = open("{0}/{1}_model.sql".format(path, self.name), "w")
        sql_model_file.write(self.generate_model_transform_sql())
        sql_model_file.close()

    def generate_schema_sql(self):
        schema_sql = ''' \n 
with {0}_source as(\n
select * from {{{{source('{0}_source', '{0}')}}}}\n
),\n
final as (select * from {0}_source)\n
select *   from final
'''.format(self.name, self.name)
        return schema_sql

    def write_model_sql(self, path):
        sql_model_file = open("{0}/{1}_model.sql".format(path, self.name), "w")
        sql_model_file.write(self.generate_model_sql())
        sql_model_file.close()

    def write_model_sql_with_schema(self, path):
        sql_model_file = open("{0}/{1}_model.sql".format(path, self.name), "w")
        sql_model_file.write(self.generate_schema_sql())
        sql_model_file.close()

    def get_yaml_dict(self):
        return {
            "name": "{0}_source".format(self.name),
            "schema": self.schema,
            "tables": [{"name": self.name}]

        }

    @staticmethod
    def generate_schema(table_list, path):
        table_yaml_list = []
        for table in table_list:
            table_yaml_list.append(table)
        yaml_dict = {
            "version": 2,
            "sources": table_yaml_list
        }

        print(yaml_dict)
        schema_file = open("{0}/schema.yml".format(path), "w")
        yaml.dump(yaml_dict, schema_file, allow_unicode=True)

    def generate_csv_file(self, path):

        columns = "table_name,column,target_column,transform\n"
        for col in self.column_list:
            columns = columns + "{0},{1},{2},None\n".format(self.name, col.name, col.name)
        sql_model_file = open("{0}/{1}.csv".format(path, self.name), "w")
        sql_model_file.write(columns)
        sql_model_file.close()
"""
with sales_model as (
    select
     'RetailerID',
    'RetailerName',
 'RetailerCity',
 'State',
 'Zone',
 'ERPName',
 'StateID',
 'City_Type_Id',
 'BrickCode',
 'TownCode',
 'Town',
 'Active'

from sales
)

select * from sales_model

"""
