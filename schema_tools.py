import generate_metaschema as gm
import pyarrow.parquet as pq
from UID import UID

numeric = 'int64 double'.split()
logical = 'timestamp string'.split()


def empty_table(dataset_name):
    # First dataset part
    dataset = gm.DataSetData(name=dataset_name, datatype='Tabular', uid=UID().str)
    tabledata = gm.TableData(name=dataset_name, uid=UID().str)
    tabular = gm.Tabular(tabledata=tabledata)
    model = gm.CaspianSchema(dataset=dataset, model=tabular)
    schema = model
    return schema


def minimal_constraints(stats, is_numeric=False):
    constraints = None
    if is_numeric:
        constraint_min = gm.Constraint(name='greater_equal', values={'warning': stats.min, 'error': ''}, enabled=False)
        constraint_max = gm.Constraint(name='less_equal', values={'warning': stats.max, 'error': ''}, enabled=False)
        constraints = [constraint_min, constraint_max]
    return constraints


def minimal_field(column, raw_name):
    field_names = gm.FieldNames(uid=UID().str, raw_name=raw_name, display_name=raw_name)
    parquet_type = column.physical_type.lower()
    if not column.is_stats_set:
        raise ImportError
    stats = column.statistics
    # print(sts.logical_type)
    logical_type = stats.logical_type.type.lower() if stats.logical_type.type != '' else parquet_type
    status = gm.Status(status='raw')
    field_types = gm.FieldTypes(pq_type=parquet_type, logical_type=logical_type, arrow_type=parquet_type,
                                representation=parquet_type)
    is_numeric = parquet_type in numeric
    field_flags = gm.FieldFlags(is_raw=True, is_numeric=is_numeric)
    constraints = minimal_constraints(stats, is_numeric)
    return gm.DataField(status=status, names=field_names, types=field_types, flags=field_flags, constraints=constraints)


def minimal_fields(pq_metadata, select_columns=None):
    row_group = pq_metadata.row_group(0)
    fields = []
    for i in range(pq_metadata.num_columns):
        column = row_group.column(i)
        raw_name = column.path_in_schema
        if select_columns is not None:
            if raw_name not in select_columns:
                continue
        fields.append(minimal_field(column, raw_name))
    return fields


def minimal_table(dataset_name, parquet_file, select_columns=None):
    # schema = empty_table(dataset_name)
    pq_metadata = pq.read_metadata(parquet_file)
    # pq_schema = pq.read_schema(parquet_file)
    # Crash out if more than one row_groups - figure this case out later  TODO
    if pq_metadata.num_row_groups != 1:
        raise ImportError
    dataset_data = gm.DataSetData(name=dataset_name, datatype='Tabular', uid=UID().str)
    status = gm.Status(status='raw')
    table_data = gm.TableData(name=dataset_name, uid=UID().str)
    fields = minimal_fields(pq_metadata, select_columns)
    table = gm.Tabular(status=status, table_data=table_data, fields=fields)
    return gm.CaspianSchema(dataset=dataset_data, model=table)


def make_field_schema(status=None, names=None, types=None, flags=None, constraints=None):
    schema = gm.DataField(status=status, names=names, types=types, flags=flags, constraints=constraints)
    return schema
