from pathlib import Path
import metadata_definition as md
import pyarrow.parquet as pq
from UID import UID

numeric = 'int int8 int16 int32 int64 uint uint8 uint16 uint32 uint64 float float16 float32 float64 double'.split()
logical = 'timestamp string'.split()
dataroot = Path.home() / 'data'


def empty_table(dataset_name):
    # First dataset part
    dataset = md.DataSetData(name=dataset_name, datatype='Tabular', uid=UID().str)
    tabledata = md.TableInfo(name=dataset_name, uid=UID().str)
    tabular = md.DataTable(tabledata=tabledata)
    model = md.MetadataDefinition(dataset=dataset, model=tabular)
    schema = model
    return schema


def minimal_constraints(stats, is_numeric=False):
    constraints = None
    if is_numeric:
        constraint_min = md.Constraint(name='greater_equal', values={'warning': stats.min, 'error': ''}, enabled=False)
        constraint_max = md.Constraint(name='less_equal', values={'warning': stats.max, 'error': ''}, enabled=False)
        constraints = [constraint_min, constraint_max]
    return constraints


def minimal_column(column, raw_name):
    field_names = md.ColumnNames(uid=UID().str, raw_name=raw_name, display_name=raw_name)
    parquet_type = column.physical_type.lower()
    if not column.is_stats_set:
        raise ImportError
    stats = column.statistics
    # print(sts.logical_type)
    logical_type = stats.logical_type.type.lower() if stats.logical_type.type != '' else parquet_type
    status = md.Status(status='raw')
    field_types = md.ColumnTypes(parquet_type=parquet_type, logical_type=logical_type, arrow_type=parquet_type,
                                 representation=parquet_type)
    is_numeric = parquet_type in numeric
    field_flags = md.ColumnFlags(is_raw=True, is_numeric=is_numeric)
    constraints = minimal_constraints(stats, is_numeric)
    return md.DataColumn(status=status, names=field_names, types=field_types, flags=field_flags, constraints=constraints)


def minimal_columns(pq_metadata, select_columns=None):
    row_group = pq_metadata.row_group(0)
    fields = []
    for i in range(pq_metadata.num_columns):
        column = row_group.column(i)
        raw_name = column.path_in_schema
        if select_columns is not None:
            if raw_name not in select_columns:
                continue
        fields.append(minimal_column(column, raw_name))
    return fields


def minimal_table(dataset_name, parquet_file, select_columns=None):
    # schema = empty_table(dataset_name)
    pq_metadata = pq.read_metadata(parquet_file)
    # pq_schema = pq.read_schema(parquet_file)
    # Crash out if more than one row_groups - figure this case out later  TODO  - should just read the first row_group
    if pq_metadata.num_row_groups != 1:
        raise ImportError
    dataset_data = md.DataSetData(name=dataset_name, datatype='Tabular', uid=UID().str)
    status = md.Status(status='raw')
    table_data = md.TableInfo(name=dataset_name, uid=UID().str)
    columns = minimal_columns(pq_metadata, select_columns)
    table = md.DataTable(status=status, table_data=table_data, columns=columns)
    return md.MetadataDefinition(dataset=dataset_data, model=table)

def get_decode_info(table, table_info):
    decode_info = {}
    if table_info['file_type'] != 'csv':
        return None
    decode_info = {'file_type': 'csv'}
    fields = 'column_names include_columns column_types delimiter null_values timestamp_parsers safe'.split()
    for f in fields:
        if f in table_info:
            decode_info[f] = table_info[f]
    return decode_info


def make_field_schema(status=None, names=None, types=None, flags=None, constraints=None):
    schema = md.DataColumn(status=status, names=names, types=types, flags=flags, constraints=constraints)
    return schema

def minimal_metadata(dataset_name, ds_info):
    dataset_dir = dataroot / dataset_name

    dataset_data = md.DataSetData(name=dataset_name, datatype='Tabular', uid=UID().str)
    tables = {}
    for table in ds_info.tables:
        table_info = ds_info.tables[table]
        status = md.Status(status='raw')
        decodeinfo = md.DecodeInfo(**get_decode_info(table, table_info))
        table_data = md.TableInfo(name=table, uid=UID().str, decodeinfo=decodeinfo)
        table_dir = dataset_dir / 'raw' / table
        dataurl = table_info.url.replace('{x}', str(table_info.default_ingestion))
        table_csv = table_dir / dataurl.split('/')[-1]
        parquet_file = table_csv.with_suffix('.parquet')
        pq_metadata = pq.read_metadata(parquet_file)
        inc_cols = table_info.get('include_columns', None)
        columns = minimal_columns(pq_metadata, inc_cols)
        tables[table] = md.DataTable(status=status, table_info=table_data, columns=columns)
    tabular = md.Tabular(status=status, tables=tables)
    return md.MetadataDefinition(dataset=dataset_data, model=tabular)



