import os
import glob
import hashlib
import json
import yaml
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from dotmap import DotMap
import generate_metaschema as gm
import schema_tools as stls
import jsonschema


def load_data(dataset_name, dataurl, columns=None):
    # Load a dataset - as an arrow table from parquet
    # If parquet file does not exist - convert csv to parquet
    csv_filename = f'data/{dataset_name}/raw/' + dataurl.split('/')[-1]
    pq_filename = csv_filename.replace('csv', 'parquet')
    if not os.path.exists(pq_filename):
        if os.path.exists(csv_filename):
            csv_to_parquet(csv_filename, pq_filename)
    # df = pd.read_parquet(fpq, columns=columns)
    pt = pq.read_table(pq_filename, columns)
    return pt, pq_filename, csv_filename


def csv_to_parquet(csv_filename, pq_filename):
    pt = csv.read_csv(csv_filename)
    pq.write_table(pt, pq_filename, version='2.6', compression='none')


def get_data_size(pq_table, pq_filename, csv_filename):
    csv_size = os.path.getsize(csv_filename) / 1000000
    pq_size = os.path.getsize(pq_filename) / 1000000
    mem_size = pq_table.nbytes / 1000000
    return csv_size, pq_size, mem_size


def get_schema(dataset_name, pq_filename, select_columns=None):
    schema_path = f'schemas/{dataset_name}.schema.json'
    if not os.path.exists(schema_path):  # Generate minimal schema if no schema exists yet
        schema = stls.minimal_table(dataset_name, pq_filename, select_columns)
        with open(f'schemas/{dataset_name}.schema.json', 'w') as f:
            f.write(schema.json(indent=2, exclude_unset=True))
    return load_dataset_schema(dataset_name)


def load_dataset_schema(dataset_name):
    schema_path = f'schemas/{dataset_name}.schema.json'
    with open(schema_path, 'r') as fh:  # Load the schema
        schema = json.load(fh)
        schema = gm.CaspianSchema(**schema)
    return schema


def get_datasets():
    with open('config/datasets.yaml') as fh:
        datasets = DotMap(yaml.safe_load(fh), _dynamic=False)
    return datasets


def get_dataset_dirs(dataset_name, ds_info=None):
    if ds_info is None:
        ds_info = get_datasets()
    dataset_dirs = {d: ds_info[d].replace('{x}', dataset_name) for d in 'dataset_dir raw_dir valid_dir'.split()}
    return dataset_dirs


def get_constraints():
    with open('config/constraints.yaml') as fh:
        constraints = DotMap(yaml.safe_load(fh), _dynamic=False)
    return constraints['constraints']


def type_constraints(typ, cons):
    tc = []
    for c in cons:
        if typ in cons[c]:
            tc.append(c)
    return tc


def get_hash(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path,"rb") as f:
        # Read and update hash string value in blocks of 4K
        i = 0
        for byte_block in iter(lambda: f.read(4096),b""):
            sha256_hash.update(byte_block)
            i += 1
    return sha256_hash.hexdigest()


def make_default_schema():
    pass


def load_schema():
    pass


def save_schema():
    pass


def get_row_data(parquet_file, selcols, dotmap=False):
    # Collect data available from the parquet file metadata and schema
    md = pq.read_metadata(parquet_file)
    sc = pq.read_schema(parquet_file)
    rd = {'num_columns': md.num_columns, 'num_rows': md.num_rows, 'num_row_groups': md.num_row_groups}
    if rd['num_row_groups'] != 1:
        raise ImportError
    rg = md.row_group(0)
    rd['fields'] = {}
    for i in range(md.num_columns):
        col = rg.column(i)
        fnam = col.path_in_schema
        if selcols is not None:
            if fnam not in selcols:
                continue
        field = {}
        field['field_name'] = fnam
        field['dtype'] = col.physical_type.lower()
        if not col.is_stats_set:
            raise ImportError
        stats = col.statistics
        field['min'] = stats.min
        field['max'] = stats.max
        field['nulls'] = '' if stats.null_count == 0 else stats.null_count
        field['logical'] = stats.logical_type.type
        field['arrowtype'] = sc.field(i).type
        field['distinct'] = ''
        field['value_counts'] = ''
        rd['fields'][fnam] = field
    if dotmap:
        rd = DotMap(rd)
    return rd


def setcolmetadata(pt, n, section, mdict, parquet_file=None):
    # Set arrow/parquet metadata for a particular column
    st = pt.schema
    sfields = [st.field(i) for i in range(len(st))]  # Make a copy of the old (immutable) fields
    oldmd = st.field(n).metadata  # Save old metadata from field we are modifying
    if oldmd is None:
        newmd = {b'caspian': json.dumps({section: mdict}).encode('utf-8')}
    else:
        newmd = json.loads(dict(oldmd)[b'caspian'].decode())  # Grab the old Caspian metadata
        if section not in newmd:
            newmd[section] = {}
        if type(mdict) == list:
            newmd[section] = mdict
        else:  # case of dict
            newmd[section].update(mdict)
        newmd = {b'caspian': json.dumps(newmd).encode()}
    sfields[n] = pa.field(st.field(n).name, st.field(n).type, metadata=newmd)
    newst = pa.schema(sfields, st.metadata)  # Build the new schema
    pt = pt.cast(newst)  # Replace the old schema
    if parquet_file is not None:
        pq.write_table(pt, parquet_file)
    return pt


def readcolmetadata(pt, n, section=None):
    # Get the column metadata from arrow/parquet schema
    md = pt.schema.field(n).metadata
    if md is None:  # Column has no metadata
        return {}
    if b'caspian' not in md:
        return {}  # Column has no Caspian metadata
    metad = json.loads(md[b'caspian'].decode())
    if not metad:
        return {}  # There was no payload
    if section is None:
        return metad  # Send back all of the metadata
    if section not in metad:
        return {}  # No metadata for the section we requested
    return metad[section]  # Send back the section metadata


def readcolmetadata2(parquet_file, n, section=None):
    # Get the column metadata from arrow/parquet schema
    md = pq.read_schema(parquet_file).field(n).metadata
    if md is None:  # Column has no metadata
        return {}
    if b'caspian' not in md:
        return {}  # Column has no Caspian metadata
    metad = json.loads(md[b'caspian'].decode())
    if not metad:
        return {}  # There was no payload
    if section is None:
        return metad  # Send back all of the metadata
    if section not in metad:
        return {}  # No metadata for the section we requested
    return metad[section]  # Send back the section metadata


def generate_schema(parquet_file):
    # Combine the column meta and add the table metadata
    pt = pq.read_table(parquet_file)
    fields = []
    for i in range(pt.num_columns):
        mdata = readcolmetadata(pt, i)
        if len(mdata) == 0:
            continue  # This field has No metadata
        fields.append(mdata)
    if len(fields) == 0:
        return None  # Found no metadata
    data_set = gm.DataSetData(name='yellow_taxi', uid='zzzzzzzz', datatype='Tabular')
    table_data = gm.TableData(name='yellow_taxi', uid='aaaaaaaa')
    tabular = gm.Tabular(tabledata=table_data, fields=fields)
    schema = gm.CaspianSchema(dataset=data_set, model=tabular)
    # validate schema
    metaschema = json.load(open('schemas/caspian_metaschema.json'))
    jsonschema.validate(schema, metaschema)
    return schema


def get_validation_data(dataset_name):
    # Load dir contents
    dirs = get_dataset_dirs(dataset_name)
    val_dir = dirs['valid_dir']
    vals = glob.glob(f'{val_dir}/valid*')
    dir_list = os.listdir(val_dir)
    vals = sorted([v for v in dir_list if v.startswith('valid')])[::-1]
    validations = {}
    for val in vals:
        val_path = f'{val_dir}/{val}'
        with open(val_path, 'r') as fh:
            val_data = json.load(fh)
        validations[val_data['ingestion']] = val_data
    return validations
    # Fill structure of validation data