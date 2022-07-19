import os
import json
import streamlit as st
import pyarrow.parquet as pq
import pandas as pd
from dotmap import DotMap
from valtools import validate_column_names, make_columns, make_ingestions, make_num_row_graph, dtypcol
from destools import coldict
from datatools import load_dataset_schema, get_datasets, get_validation_data, get_n_rows_to_df

# Streamlit setup
st.set_page_config(page_title="Asset validation", page_icon="🔑", layout="wide")
st.markdown("""<style>.css-18e3th9 {padding-top: 1rem;padding-bottom: 0rem;padding-left: 2rem;padding-right: 2rem;}
</style>""", unsafe_allow_html=True)
st.subheader('Asset ingestion validation ')

# Set vars
dataset_name = 'yellow_taxi'
raw_path = f'data/{dataset_name}/raw/'
work_path = f'data/{dataset_name}/raw/'
ingestion = '2021-07'

col_map = {0: 'Green', 1: 'Orange', 2: 'Red'}

# Read Schema
schema = load_dataset_schema(dataset_name)
schema_cols = [schema.model.fields[i].names.raw_name for i in range(len(schema.model.fields))]
num_schema_cols = len(schema_cols)

ds_data = get_datasets().datasets[dataset_name]
work_path = f'data/{dataset_name}/raw/'
pq_filename = work_path + ds_data.pattern.replace('{x}', ingestion) + '.parquet'
df = get_n_rows_to_df(pq_filename, 20)
st.dataframe(df.style.applymap(dtypcol), height=200)

#pt = pq.read_table(pq_filename)
#df = pt.slice(0, 20).to_pandas()
#st.dataframe(df, height=200)
#st.dataframe(df.style.applymap(dtypcol), height=200)

# Check if previous validations
validations = get_validation_data(dataset_name)
# Load previous vals

# Check cols, names, order
with open(f'data/{dataset_name}/valid.json', 'r') as fh:
    valid = DotMap(json.load(fh))

coldict({'Table': dataset_name}, 'Black', col_map[valid.table.table_level], back='White')
validate_column_names(valid)

# Check rows
# make_columns(valid, schema)
rows = []
vs = []
for v in validations:
    vs.append(validations[v]['ingestion'])
    rows.append(validations[v]['table']['num_rows'])

# make_num_row_graph(list(reversed(rows)), list(reversed(vs)))
dfp = pd.DataFrame(list(reversed(rows)), index=list(reversed(vs)), columns=['Num Rows'])
# st.write(dfp)
st.line_chart(dfp, height=400)

# Check ingestions
make_ingestions(validations, schema)

# Loop over fields

## Check dtypes

## Loop over constraints

### Check constraints

### Fill Flags

### Fill validations datastructure

### Do analysis

# Display validation results

# Do table constrints

# Do table analysis


st.write(dict(validations['2021-07']))




