import json
import pyarrow.parquet as pq
from datatools import load_dataset_schema, get_datasets, get_row_data
from constraints_arrow import check_constraint

getclass Validation(object):

    def __init__(self, dataset_name, schema):
        self.dataset_name = dataset_name
        self.schema = schema
        self.valid = {}

    def get_dataset(self):
        pass

    def validate_table(self):
        self.valid['table'] = {}
        # get the table
        self.validate_table_columns()
        # data_table = pq.read_table(self.pq_filename)
        pq_metadata = get_row_data(self.pq_filename, self.schema_cols, dotmap=True)
        table = self.valid['table']
        table_level = table['column_name_level']
        table['num_rows'] = pq_metadata.num_rows
        table['column_checks'] = {}
        column_checks = table['column_checks']
        for i, column_name in enumerate(self.schema_cols):
            #print(' ')
            print(f'{column_name}, ')
            j = self.data_cols.index(column_name)
            data_table = pq.read_table(self.pq_filename, columns=[column_name])
            data_table_column = data_table.column(0)
            column_checks[column_name] = self.validate_column(i, data_table_column.combine_chunks(),
                                                    pq_metadata['fields'][column_name], self.schema.model.fields[i])
            table_level = max(table_level, column_checks[column_name]['column_level'])
        table['table_level'] = table_level
        return table_level


    def validate_column(self, i, pa_col, metadata, schema):
        #print(i)
        #print(len(pa_col))
        #print(metadata)
        #print(schema)
        column_check = {}
        column_level = 0
        # Validate type
        if schema.types.pq_type != metadata.dtype:
            column_check['parquet_type'] = {'schema_type': schema.types.pq_type, 'parquet_type': metadata.dtype}
            column_level = 2
        if schema.types.logical_type != metadata.logical.lower():
            column_check['logical_type'] = {'schema_type': schema.types.logical_type,
                                            'parquet_type': metadata.logical.lower()}
            column_level = 2
        if column_check == {}:
            column_check['types_validated'] = True
        # Check Nulls
        if metadata.nulls == '':
            column_check['nulls_validated'] = True
        else:
            column_check['null_entries'] = {'count': int(metadata.nulls),
                                            'percent': float(metadata.nulls) / float(len(pa_col)) * 100.}
            column_level = 1
        # Check constraints
        constraint_results = []
        constraint_level = 0
        for cons in schema.constraints:
            if cons.enabled != True:
                continue
            cons_level, cons_result = check_constraint(pa_col, cons)
            constraint_level = max(constraint_level, cons_level)
            constraint_results.append(cons_result)
        column_check['constraint_level'] = constraint_level
        column_check['constraints'] = constraint_results
        column_level = max(column_level, constraint_level)
        column_check['column_level'] = column_level
        return column_check


    def validate_table_columns(self):
        # Get columns from parquet file
        table = self.valid['table']
        out = {}
        col_level = 0
        pq_schema = pq.read_schema(self.pq_filename)
        data_cols = pq_schema.names
        # Tests
        # data_cols = data_cols[:2] + data_cols[4:]  # Missing cols
        # data_cols += ['poo']  # Extra cols
        # data_cols[3] = 'poo'  # Wrong cols
        # data_cols = list(reversed(data_cols))  # Different cols
        num_data_cols = len(data_cols)
        table['num_data_cols'] = num_data_cols
        table['data_cols'] = data_cols
        # Get columns from schema
        schema_fields = self.schema.model.fields
        schema_cols = [schema_fields[i].names.raw_name for i in range(len(schema_fields))]
        num_schema_cols = len(schema_cols)
        table['num_schema_cols'] = num_schema_cols
        table['schema_cols'] = schema_cols
        if num_data_cols < num_schema_cols:
            out['missing_columns'] = [c for c in schema_cols if c not in data_cols]
            col_level = 2
        if num_data_cols > num_schema_cols:
            out['extra_columns'] = [c for c in data_cols if c not in schema_cols]
            col_level = max(col_level, 1)
        if num_data_cols == num_schema_cols:
            if set(schema_cols) != set(data_cols):
                out['different_columns'] = {'schema_columns': schema_cols, 'data_columns': data_cols}
                col_level = 2
            else:
                if schema_cols == data_cols:
                    out['columns_validated'] = True
                else:
                    out['column_order'] = {'schema_columns': schema_cols, 'data_columns': data_cols}
                    col_level = max(col_level, 1)
        self.valid['table']['column_name_checks'] = out
        self.valid['table']['column_name_level'] = col_level
        self.schema_cols = schema_cols
        self.data_cols = data_cols

    def validate(self, pq_filename):
        # self.ingestion = ingestion
        self.pq_filename = pq_filename
        self.dataset_type = self.schema.dataset.datatype
        match self.dataset_type:
            case 'Tabular':
                level = self.validate_table()
            case 'Relational':
                level = self.validate_relational()
        self.valid['validated'] = level
        return self.valid



if __name__ == '__main__':
    dataset_name = 'yellow_taxi'
    ingestion = '2021-07'
    ds_data = get_datasets().datasets[dataset_name]
    work_path = f'data/{dataset_name}/raw/'
    pq_filename = work_path + ds_data.pattern.replace('{x}', ingestion) + '.parquet'
    schema = load_dataset_schema(dataset_name)
    vd = Validation(dataset_name, schema)
    valid = vd.validate(pq_filename)
    with open(f'data/{dataset_name}/valid.json', 'w') as fh:
        json.dump(valid, fh, indent=2)
    #print(valid)

"""
Automate - where possible
Inspect - where necessary
Expose - where requested
"""
