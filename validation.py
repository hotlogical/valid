import json
import pyarrow.parquet as pq
from datatools import load_dataset_metadata, get_datasets, get_row_data
from constraints_arrow import check_constraint

class Validation(object):

    def __init__(self, dataset_name, table_name, metadata):
        self.dataset_name = dataset_name
        self.table_name = table_name
        self.metadata = metadata
        self.valid = {}

    def get_dataset(self):
        pass

    def validate_table(self):
        self.valid['table'] = {}
        # get the table
        self.validate_table_columns()
        # data_table = pq.read_table(self.pq_filename)
        pq_metadata = get_row_data(self.pq_filename, self.metadata_cols, dotmap=True)
        table = self.valid['table']
        table_level = table['column_name_level']
        table['num_rows'] = pq_metadata.num_rows
        table['column_checks'] = {}
        column_checks = table['column_checks']
        for i, column_name in enumerate(self.metadata_cols):
            #print(' ')
            print(f'{column_name}, ')
            j = self.data_cols.index(column_name)
            data_table = pq.read_table(self.pq_filename, columns=[column_name])
            data_table_column = data_table.column(0)
            column_checks[column_name] = self.validate_column(i, data_table_column.combine_chunks(),
                                                              pq_metadata['fields'][column_name],
                                                              self.metadata.model.tables[self.table_name].columns[i])
            table_level = max(table_level, column_checks[column_name]['column_level'])
        table['table_level'] = table_level
        return table_level


    def validate_column(self, i, pa_col, pq_metadata, metadata):
        #print(i)
        #print(len(pa_col))
        #print(pq_metadata)
        #print(metadata)
        column_check = {}
        column_level = 0
        # Validate type
        if metadata.types.parquet_type != pq_metadata.dtype:
            column_check['parquet_type'] = {'metadata_type': metadata.types.parquet_type, 'parquet_type': pq_metadata.dtype}
            column_level = 2
        if metadata.types.logical_type != pq_metadata.logical.lower():
            column_check['logical_type'] = {'metadata_type': metadata.types.logical_type,
                                            'parquet_type': pq_metadata.logical.lower()}
            column_level = 2
        if column_check == {}:
            column_check['types_validated'] = True
        # Check Nulls
        if pq_metadata.nulls == '':
            column_check['nulls_validated'] = True
        else:
            column_check['null_entries'] = {'count': int(pq_metadata.nulls),
                                            'percent': float(pq_metadata.nulls) / float(len(pa_col)) * 100.}
            column_level = 1
        # Check constraints
        constraint_results = []
        constraint_level = 0
        if metadata.constraints is not None:
            for cons in metadata.constraints:
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
        # Get columns from metadata
        metadata_fields = self.metadata.model.tables[self.table_name].columns
        metadata_cols = [metadata_fields[i].names.raw_name for i in range(len(metadata_fields))]
        num_metadata_cols = len(metadata_cols)
        table['num_metadata_cols'] = num_metadata_cols
        table['metadata_cols'] = metadata_cols
        if num_data_cols < num_metadata_cols:
            out['missing_columns'] = [c for c in metadata_cols if c not in data_cols]
            col_level = 2
        if num_data_cols > num_metadata_cols:
            out['extra_columns'] = [c for c in data_cols if c not in metadata_cols]
            col_level = max(col_level, 1)
        if num_data_cols == num_metadata_cols:
            if set(metadata_cols) != set(data_cols):
                out['different_columns'] = {'metadata_columns': metadata_cols, 'data_columns': data_cols}
                col_level = 2
            else:
                if metadata_cols == data_cols:
                    out['columns_validated'] = True
                else:
                    out['column_order'] = {'metadata_columns': metadata_cols, 'data_columns': data_cols}
                    col_level = max(col_level, 1)
        self.valid['table']['column_name_checks'] = out
        self.valid['table']['column_name_level'] = col_level
        self.metadata_cols = metadata_cols
        self.data_cols = data_cols

    def validate(self, pq_filepath):
        # self.ingestion = ingestion
        self.pq_filename = pq_filepath
        self.dataset_type = self.metadata.dataset.datatype
        match self.dataset_type:
            case 'Tabular':
                level = self.validate_table()
        self.valid['validated'] = level
        return self.valid



if __name__ == '__main__':
    dataset_name = 'yellow_taxi'
    ingestion = '2021-07'
    ingestion = 'lookup'
    table_name = 'yellow_taxi'
    table_name = 'taxi_zones'
    ds_data = get_datasets().datasets[dataset_name]
    from pathlib import Path
    dataroot = Path.home() / 'data'
    work_path = dataroot / dataset_name / 'raw' / table_name
    filename = ds_data.tables[table_name].pattern.replace('{x}', ingestion) + '.parquet'
    pq_filepath = work_path / filename
    metadata = load_dataset_metadata(dataset_name)
    vd = Validation(dataset_name, table_name, metadata)
    valid = vd.validate(pq_filepath)
    testfile = dataroot / dataset_name / f'{table_name}.valid.json'
    with testfile.open('w') as fh:
        json.dump(valid, fh, indent=2)
    #print(valid)

"""
Automate - where possible
Inspect - where necessary
Expose - where requested
"""
