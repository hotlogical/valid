import json
import jsonschema
import streamlit as st
from datatools import load_data, get_datasets, get_row_data, get_schema, get_file_info, get_n_rows_to_df
from destools import make_fields, dtypcol, colbox, coldict, make_table

# Must be first command executed
st.set_page_config(page_title="Asset description tool", page_icon="🔑", layout="wide")
st.markdown("""<style>.css-18e3th9 {padding-top: 1rem;padding-bottom: 0rem;padding-left: 2rem;padding-right: 2rem;}
</style>""", unsafe_allow_html=True)
st.subheader('Asset description tool')


# Define datasets and choose one
ccols = st.columns((2, 2, 1, 8))
selcols = None
ds_data = get_datasets()
datasets = list(ds_data.datasets.keys())
if 'old_dataset_name' not in st.session_state:
    st.session_state.old_dataset_name = ''
with ccols[0]:  # dataset chooser dropdown
    dataset = datasets[0]
    dataset_name = st.selectbox('Choose dataset', datasets, datasets.index(dataset))
    info = ds_data.datasets[dataset_name]
    if dataset_name != st.session_state.old_dataset_name:  # Reset if change dataset
        st.experimental_memo.clear()
        st.session_state.old_dataset_name = dataset_name
with ccols[1]:  # Ingestion chooser dropdown
    ingestion_default = info.default
    ingestions = info.ingestions
    ingestion = st.selectbox('Choose ingestion', ingestions, ingestions.index(ingestion_default))
    dataurl = info.url.replace('{x}', ingestion)
    if 'columns' in info:
        selcols = info.columns
    file_info = get_file_info(dataset_name, dataurl, columns=selcols)
with ccols[3]:
    st.caption(f'Getting dataset {ingestion} from {dataurl}')
    fi = file_info
    schema = get_schema(dataset_name, fi.parquet, selcols)
    coldict({'Name': schema.dataset.name, 'DataType': schema.dataset.datatype, 'UID': schema.dataset.uid})
    coldict({f'{fi.pq_num_rows:,}': 'rows', fi.pq_num_cols: 'columns', 'On disk': f'{fi.csv_size:.1f} MB (raw)',
             f'{fi.parquet_size:.1f}': 'MB (parquet)', f'{fi.mem_size:.1f}': 'MB (arrow)'}, 'Grey', 'Grey')

# Table and Field tabs
st.write('<style>div.row-widget.stRadio > div{flex-direction:row;} </style> ', unsafe_allow_html=True)
tab = st.radio('', ['Table ___', 'Fields'])

# Get raw metadata from the parquet file
row_data = get_row_data(file_info.parquet, file_info.columns)

if tab == 'Table ___':  # Show graph of data and some stats
    # Show a dataframe of the first n rows
    td = schema.model.table_data
    coldict({'Table name': td.name, 'UID': td.uid})
    # df = pt.slice(0, 20).to_pandas()
    df = get_n_rows_to_df(file_info.parquet, 20)
    st.dataframe(df.style.applymap(dtypcol), height=200)
    # Make table constraints and transforms section
    # make_table(row_data['fields'], pt)

if tab == 'Fields':
    # Make the stats and schema sections for each field
    schema = make_fields(row_data['fields'], file_info.parquet, schema)
st.markdown('---')

# Validate and show schema
colbox('Schema', 'Green')
st.write("![ready](https://img.shields.io/static/v1?label=Status&message=Complete&color=005500)")
# scj = generate_schema(parquet_file)
if schema is not None:
    st.write(schema.dict())
    with open(f'schemas/{dataset_name}.schema.json', 'w') as fh:
        fh.write(schema.json(indent=2, exclude_unset=True))
    # validate schema
    # metaschema = json.load(open('schemas/caspian_metaschema.json'))
    # res = None
    # res = jsonschema.validate(schema, metaschema)
    # st.write('Schema validation : ', res)

st.caption('Where data becomes knowledge')
