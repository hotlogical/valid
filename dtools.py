import os
import json
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import generate_metaschema as gm
import jsonschema


def load_data(dataurl, columns=None):
    # Load a dataset - get size stats
    fpq = 'data/' + dataurl.split('/')[-1].replace('.csv', '') + '.parquet'
    print(fpq)
    fcsv = fpq.replace('parquet', 'csv')
    print(fcsv)
    if not os.path.exists(fpq):
        if os.path.exists(fcsv):
            dataurl = fcsv
        pt = csv.read_csv(dataurl)
        pq.write_table(pt, fpq, version='2.6', compression='none')
    # df = pd.read_parquet(fpq, columns=columns)
    pt = pq.read_table(fpq, columns)
    msize = pt.nbytes / 1000000
    rsize = os.path.getsize(fcsv) / 1000000
    pqsize = os.path.getsize(fpq) / 1000000
    return fpq, pt, rsize, pqsize, msize

def get_row_data(parquet_file, selcols):
    # Collect data available from the parquet file metadata and schema
    md = pq.read_metadata(parquet_file)
    sc = pq.read_schema(parquet_file)
    rd = {}
    rd['num_columns'] = md.num_columns
    rd['num_rows'] = md.num_rows
    rd['num_row_groups'] = md.num_row_groups
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
    if not b'caspian' in md:
        return {}  # Column has no Caspian metadata
    metad = json.loads(md[b'caspian'].decode())
    if not metad:
        return {}  # There was no payload
    if section is None:
        return metad  # Send back all of the metadata
    if not section in metad:
        return {}  # No metadata for the section we requested
    return metad[section]  # Send back the section metadata

def readcolmetadata2(parquet_file, n, section=None):
    # Get the column metadata from arrow/parquet schema
    md = pq.read_schema(parquet_file).field(n).metadata
    if md is None:  # Column has no metadata
        return {}
    if not b'caspian' in md:
        return {}  # Column has no Caspian metadata
    metad = json.loads(md[b'caspian'].decode())
    if not metad:
        return {}  # There was no payload
    if section is None:
        return metad  # Send back all of the metadata
    if not section in metad:
        return {}  # No metadata for the section we requested
    return metad[section]  # Send back the section metadata

def generate_schema(parquet_file):
    # Combine the column meta and add the table metadata
    pt = pq.read_table(parquet_file)
    fields = []
    for i in range(pt.num_columns):
        colname = pt.column_names[i]
        mdata = readcolmetadata(pt, i)
        if len(mdata) == 0:
            continue  # This field has No metadata
        fields.append(mdata)
    if len(fields) == 0:
        return None  # Found no metadata
    tabular = gm.Tabular(fields=fields)
    model = gm.CaspianSchema(model=tabular)
    # validate schema
    #metaschema = json.load(open('schemas/caspian_metaschema.json'))
    #jsonschema.validate(model, metaschema)
    return model

    #return json.dumps(sc, indent=2)
