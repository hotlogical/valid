import json
import jsonschema
import streamlit as st
from datatools import load_data, get_datasets, get_row_data, get_data_size, get_schema
from destools import make_fields, dtypcol, colbox, coldict, make_table

# Must be first command executed
st.set_page_config(page_title="Asset description tool", page_icon="🔑", layout="wide", )
st.markdown("""<style>.css-18e3th9 {padding-top: 0rem;padding-bottom: 0rem;padding-left: 2rem;padding-right: 2rem;}
</style>""", unsafe_allow_html=True)
st.subheader('Asset description tool')


# Define datasets and choose one
ccols = st.columns((2, 1, 8))
selcols = None
ds_data = get_datasets()
datasets = list(ds_data.datasets.keys())
with ccols[0]:  # dataset chooser dropdown
    dataset = datasets[0]
    dataset_name = st.selectbox('Choose dataset', datasets, datasets.index(dataset))
    info = ds_data.datasets[dataset_name]
    ingestion = info.default
    dataurl = info.url.replace('{x}', ingestion)
    if 'columns' in info:
        selcols = info.columns
with ccols[2]:
    st.caption(f'Getting dataset {ingestion} from {dataurl}')
    pt, pq_file, csv_file = load_data(dataset_name, dataurl, selcols)
    rsize, pqsize, msize = get_data_size(pt, pq_file, csv_file)
    ncols, nrows = pt.num_columns, pt.num_rows
    schema = get_schema(dataset_name, pq_file, selcols)
    coldict({'Name': schema.dataset.name, 'DataType': schema.dataset.datatype, 'UID': schema.dataset.uid})
    coldict({f'{nrows:,}': 'rows', ncols: 'columns', 'On disk': f'{rsize:.1f} MB (raw)',
             f'{pqsize:.1f}': 'MB (parquet)', f'{msize:.1f}': 'MB (arrow)'}, 'Grey', 'Grey')

# Table and Field tabs
st.write('<style>div.row-widget.stRadio > div{flex-direction:row;} </style> ', unsafe_allow_html=True)
tab = st.radio('', ['Table ___', 'Fields'])

# Get raw metadata from the parquet file
row_data = get_row_data(pq_file, selcols)

if tab == 'Table ___':  # Show graph of data and some stats
    # Show a dataframe of the first n rows
    td = schema.model.table_data
    coldict({'Table name': td.name, 'UID': td.uid})
    df = pt.slice(0, 20).to_pandas()
    st.dataframe(df.style.applymap(dtypcol), height=200)
    # Make table constraints and transforms section
    make_table(row_data['fields'], pt)

if tab == 'Fields':
    # Make the stats and schema sections for each field
    schema = make_fields(row_data['fields'], pt, pq_file, schema)
st.markdown('---')

# Validate and show schema
colbox('Schema', 'Green')
st.write("![ready](https://img.shields.io/static/v1?label=Status&message=Complete&color=005500)")
# scj = generate_schema(parquet_file)
if schema is not None:
    st.write(schema.dict())
    with open(f'schemas/{dataset}.schema.json', 'w') as f:
        f.write(schema.json(indent=2, exclude_unset=True))
    # validate schema
    # metaschema = json.load(open('schemas/caspian_metaschema.json'))
    # res = None
    # res = jsonschema.validate(schema, metaschema)
    # st.write('Schema validation : ', res)

st.caption('Where data becomes knowledge')
