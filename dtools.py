import os
import json
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import generate_metaschema as gm
import streamlit as st

#@st.cache
def load_data(dataurl, columns=None):
    fpq = 'data/' + dataurl.split('/')[-1].replace('.csv', '') + '.parquet'
    fcsv = fpq.replace('parquet', 'csv')
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
    st = pt.schema
    sfields = [st.field(i) for i in range(len(st))]  # copy fields we are not changing
    oldmd = st.field(n).metadata  # Get old metadata from field we are changing
    # print('oldmd ', oldmd)
    if oldmd is None:
        newmd = {b'caspian': json.dumps({section: mdict}).encode('utf-8')}
    else:
        newmd = json.loads(dict(oldmd)[b'caspian'].decode())
        print('newmd ', newmd)
        # print('newmd[names] ', newmd['names'])
        if section not in newmd:
            newmd[section] = {}
        newmd[section].update(mdict)
        newmd = {b'caspian': json.dumps(newmd).encode()}
        # print('newmd again ', newmd)
    sfields[n] = pa.field(st.field(n).name, st.field(n).type, metadata=newmd)
    newst = pa.schema(sfields, st.metadata)
    pt = pt.cast(newst)
    if parquet_file is not None:
        pq.write_table(pt, parquet_file)
    return pt

def readcolmetadata(pt, n, section=None):
    md = pt.schema.field(n).metadata
    if md is None:
        return {}
    if not b'caspian' in md:
        return {}
    metad = json.loads(md[b'caspian'].decode())
    if not metad:
        return {}
    if section is None:
        return metad
    if not section in metad:
        return {}
    return metad[section]

def generate_schema(parquet_file):
    pt = pq.read_table(parquet_file)
    fields = []
    for i in range(pt.num_columns):
        colname = pt.column_names[i]
        mdata = readcolmetadata(pt, i)
        if len(mdata) == 0:
            continue
        fields.append(mdata)
    if len(fields) == 0:
        return None
    tabular = gm.Tabular(fields=fields)
    model = gm.CaspianSchema(model=tabular)
    return model

    #return json.dumps(sc, indent=2)
