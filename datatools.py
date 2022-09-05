import os
import glob
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, List, Dict
import hashlib
import json
import yaml
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pa_csv
from dotmap import DotMap
import metadata_definition as md
import minimal_metadata as stls
import jsonschema
import streamlit as st
from arrow_types import arrow_types

dataroot = Path.home() / 'data'

@dataclass
class FileInfo:
    url: str
    csv: str
    parquet: str
    csv_size: float = 0
    parquet_size: float = 0
    mem_size: float = 0
    pq_num_cols = 0
    pq_num_rows = 0
    columns: Optional[List] = field(default_factory=lambda: None)
    types: Optional[Dict] = field(default_factory=lambda: None)
    def get_csv_size(self):
        self.csv_size = os.path.getsize(self.csv) / 1000000
    def get_parquet_size(self):
        self.parquet_size = os.path.getsize(self.parquet) / 1000000
    def get_mem_size(self):
        if not os.path.exists(self.parquet):
            csv_to_parquet(self.csv, self.parquet, self.columns)
        pt = read_parquet(self.parquet, columns=self.columns)
        self.mem_size = pt.nbytes / 1000000
        self.pq_num_cols = pt.num_columns
        self.pq_num_rows = pt.num_rows


@st.experimental_memo
def get_file_info(dataset_name, table_name, dataurl, columns=None):
    #csv_filename = f'data/{dataset_name}/raw/' + dataurl.split('/')[-1]
    #parquet_filename = csv_filename.replace('csv', 'parquet')

    dataset_dir = dataroot / dataset_name
    table_dir = dataset_dir / 'raw' / table_name
    # dataurl = table_info.url.replace('{x}', str(table_info.default_ingestion))
    csv_filename = table_dir / dataurl.split('/')[-1]
    parquet_filename = csv_filename.with_suffix('.parquet')

    file_info = FileInfo(dataurl, csv_filename, parquet_filename, columns=columns)
    file_info.get_csv_size()
    file_info.get_mem_size()
    file_info.get_parquet_size()
    return file_info

# @st.experimental_memo
def load_data(dataset_name, dataurl, columns=None):
    # Load a dataset - as an arrow table from parquet
    # If parquet file does not exist - convert csv to parquet
    csv_filename = f'data/{dataset_name}/raw/' + dataurl.split('/')[-1]
    pq_filename = csv_filename.replace('csv', 'parquet')
    if not os.path.exists(pq_filename):
        if os.path.exists(csv_filename):
            csv_to_parquet(csv_filename, pq_filename)
        # if os.path.exists(f'{csv_filename}.zip'):
        #     with zipfile.ZipFile(f'{csv_filename}.zip', 'r') as fh:
        #         csv_to_parquet(fh, pq_filename)
    # df = pd.read_parquet(fpq, columns=columns)
    pt = pq.read_table(pq_filename, columns)
    return pt, pq_filename, csv_filename

def read_parquet(pq_filename, columns=None):
    pt = pq.read_table(pq_filename, columns=columns)
    return pt

def get_n_rows_to_df(pq_filename, num_rows=10):
    pf = pq.ParquetFile(pq_filename)
    first_ten_rows = next(pf.iter_batches(batch_size=num_rows))
    df = pa.Table.from_batches([first_ten_rows]).to_pandas()
    return df

def csv_to_parquet(csv_filename, pq_filename, select_columns=None, data_types=None):
    convert_options = pa_csv.ConvertOptions(column_types=data_types, include_columns=select_columns)
    pt = pa_csv.read_csv(csv_filename, convert_options=convert_options)
    pq.write_table(pt, pq_filename, version='2.6', compression='none')
    mem_size = pt.nbytes / 1000000
    return mem_size

@dataclass
class CSVHandler:
    csv_file: str
    pq_file: Optional[str] = None
    delimiter: Optional[str] = None
    null_values: Optional[List[str]] = field(default_factory=lambda: None)
    column_names: Optional[List[str]] = field(default_factory=lambda: None)
    column_types: Optional[Dict[str, str]] = field(default_factory=lambda: None)
    timestamp_parsers: Optional[List[str]] = field(default_factory=lambda: None)
    include_columns: Optional[List[str]] = field(default_factory=lambda: None)
    safe: bool = True
    def read_csv(self):
        parse_ops = {}
        read_ops = {}
        convert_ops = {}
        if self.delimiter is not None:
            parse_ops['delimiter'] = self.delimiter
        if self.column_names is not None:
            read_ops['column_names'] = self.column_names
        if self.null_values is not None:
            convert_ops['null_values'] = self.null_values
        new_types = {}
        if self.column_types is not None:
            new_types = {c: arrow_types[self.column_types[c]] for c in self.column_types}
        if self.timestamp_parsers is not None:
            convert_ops['timestamp_parsers'] = self.timestamp_parsers
        if self.include_columns is not None:
            convert_ops['include_columns'] = self.include_columns
        all_ops = {}
        if len(parse_ops) > 0:
            all_ops['parse_options'] = pa_csv.ParseOptions(**parse_ops)
        if len(read_ops) > 0:
            all_ops['read_options'] = pa_csv.ReadOptions(**read_ops)
        if len(convert_ops) > 0:
            all_ops['convert_options'] = pa_csv.ConvertOptions(**convert_ops)
        self.pt = pa_csv.read_csv(self.csv_file, **all_ops)
        # Now convert any mis-inferred field types (must be a nicer way of doing this)
        if len(new_types) > 0:
            new_schema = []
            for f in self.pt.schema:
                if f.name in self.column_types.keys():
                    new_schema.append(pa.field(f.name, new_types[f.name]))
                else:
                    new_schema.append(pa.field(f.name, f.type))
            new_schema = pa.schema(new_schema)
            self.pt = self.pt.cast(new_schema, safe=self.safe)  # TODO Not sure how unsafe this is !
        print(self.pt.schema)
        return self.pt
    def write_parquet(self, pq_file, version='2.6', compression='none'):
        pq.write_table(self.pt, pq_file, version=version, compression=compression)

def parquet_from_yaml(dataset_name, ds_data):
    ds_info = ds_data.datasets[dataset_name]
    dataset_dir = dataroot / dataset_name
    for table in ds_info.tables:
        table_info = ds_info.tables[table]
        file_type = table_info.file_type
        if file_type != 'csv':
            print('file_type for table %s is not csv' % table)
            continue
        table_dir = dataset_dir / 'raw' / table
        dataurl = table_info.url.replace('{x}', str(table_info.default_ingestion))
        table_csv = table_dir / dataurl.split('/')[-1]
        table_parquet = table_csv.with_suffix('.parquet')
        if table_parquet.exists():
            continue
        csv_ops = {}
        fields = 'column_names include_columns column_types delimiter null_values timestamp_parsers safe'.split()
        for f in fields:
            if f in table_info:
                csv_ops[f] = table_info[f]
        ch = CSVHandler(table_csv, **csv_ops)
        ch.read_csv()
        ch.write_parquet(table_parquet)

def get_table_file(dataset_name, table, table_info):
    dataset_dir = dataroot / dataset_name
    table_dir = dataset_dir / 'raw' / table
    dataurl = table_info.url.replace('{x}', str(table_info.default_ingestion))
    table_csv = table_dir / dataurl.split('/')[-1]
    table_parquet = table_csv.with_suffix('.parquet')
    return table_parquet

def parquet_from_metadata(table_metadata):
    pass

def get_metadata(dataset_name, ds_info):
    dataset_dir = dataroot / dataset_name
    metadata_path_json = dataset_dir / 'metadata' / f'{dataset_name}.metadata.json'
    metadata_path_yaml = dataset_dir / 'metadata' / f'{dataset_name}.metadata.yaml'
    if (not metadata_path_json.exists()) and (not metadata_path_yaml.exists()):  # Generate minimal metadata if no metadata exists yet
        metadata = stls.minimal_metadata(dataset_name, ds_info)
        with metadata_path_json.open('w') as fh:
            fh.write(metadata.json(indent=2, exclude_unset=True))
        with metadata_path_yaml.open('w') as fh:
            fh.write(metadata.yaml(exclude_unset=True))
    return load_dataset_metadata(dataset_name)


def load_dataset_metadata(dataset_name):
    dataset_dir = dataroot / dataset_name
    metadata_path = dataset_dir / 'metadata' / f'{dataset_name}.metadata.json'
    metadata_path = dataset_dir / 'metadata' / f'{dataset_name}.metadata.yaml'
    with metadata_path.open('r') as fh:  # Load the metadata
        # metadata = json.load(fh)
        # metadata = md.MetadataDefinition(**metadata)
        yml = fh.read()
        metadata = md.MetadataDefinition.parse_raw(yml)
    return metadata


def get_datasets():
    with open('config/datasets.yaml') as fh:
        datasets = DotMap(yaml.safe_load(fh), _dynamic=False)
    return datasets


def get_dataset_dirs(dataset_name, ds_info=None):
    if ds_info is None:
        ds_info = get_datasets()
    dataset_dirs = {d: ds_info[d].replace('{x}', dataset_name) for d in
                    'dataset_dir raw_dir valid_dir ingested_dir'.split()}
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
#    if rd['num_row_groups'] != 1:
#        raise ImportError
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
    st = pt.metadata
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
    md = pt.metadata.field(n).metadata
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
    data_set = md.DataSetData(name='yellow_taxi', uid='zzzzzzzz', datatype='Tabular')
    table_data = md.TableInfo(name='yellow_taxi', uid='aaaaaaaa')
    tabular = md.DataTable(tabledata=table_data, columns=fields)
    schema = md.MetadataDefinition(dataset=data_set, model=tabular)
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

if __name__ == '__main__':
    #pt = read_csv('data/custom_weather/test_locations.csv', 'poo', delimiter='|',
    #              column_names='station_id station_name region_code country_ISO2 latitude longitude timezone altitude nothing'.split(),
    #              include_columns='station_id station_name region_code country_ISO2 latitude longitude timezone altitude'.split(),
    #              column_types={'altitude': 'int32'})
    ch = CSVHandler('data/custom_weather/test_locations.csv', delimiter='|',
                    column_names='station_id station_name region_code country_ISO2 latitude longitude timezone altitude nothing'.split(),
                    include_columns='station_id station_name region_code country_ISO2 latitude longitude timezone altitude'.split(),
                    column_types={'altitude': 'int32'})
    print(ch.csv_file)
    print(ch.delimiter)
    pt = ch.read_csv()
    print(pt.schema)
    print(pt.to_pandas().head(5))
