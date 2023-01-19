class Column:

    def __init__(self, name, order, datatype, default_value, transform=None, target_name=None):
        self.name = name
        self.order = order
        self.datatype = datatype
        self.default_value = default_value
        self.transform = transform
        self.target_name = target_name

    def get_column_sql(self):

        if self.transform == 'None' and self.target_name is not None:
            return self.name + " as " + self.target_name
        elif self.target_name is not None:
            return self.transform + ' as ' + self.target_name
        else:
            return self.name


"""
  "select_table_names": [ {"table":{ "name":"table_name",
    "columns": [ {"name":asv, "transform" : "distinct(asv)", "target":"asv_distinct"
  }  }],
"""