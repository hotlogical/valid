import json
import jsonschema
import streamlit as st
import pyarrow.dataset as ds
from datatools import get_datasets, get_row_data, get_metadata, get_file_info, parquet_from_yaml, get_table_file, dataroot
from destools import make_fields, dtypcol, colbox, coldict
from deepdiff import DeepDiff

# Must be first command executed
st.set_page_config(page_title="Asset description tool", page_icon="🔑", layout="wide")
st.markdown("""<style>.css-18e3th9 {padding-top: 1rem;padding-bottom: 0rem;padding-left: 2rem;padding-right: 2rem;}
</style>""", unsafe_allow_html=True)
st.subheader('Asset description tool')


# Define datasets and choose one
ccols = st.columns((3, 0.5, 12))
selcols = None
ds_data = get_datasets()
datasets = list(ds_data.datasets.keys())
if 'old_dataset_name' not in st.session_state:
    st.session_state.old_dataset_name = ''
with ccols[0]:  # dataset chooser dropdown
    dataset = datasets[0]
    dataset_name = st.selectbox('Choose dataset', datasets, datasets.index(dataset))
    ds_info = ds_data.datasets[dataset_name]
    if dataset_name != st.session_state.old_dataset_name:  # Reset if change dataset
        st.experimental_memo.clear()
        st.session_state.old_dataset_name = dataset_name
parquet_from_yaml(dataset_name, ds_data)
metadata = get_metadata(dataset_name, ds_info)
# Get list of table names and tables
tables = ds_info.tables
table_list = list(tables.keys())
with ccols[2]:
    st.write('.')
    # coldict({'Name': metadata.dataset.name, 'DataType': metadata.dataset.datatype, 'UID': metadata.dataset.uid})
    coldict({'Name': dataset_name, 'DataType': 'Tabular', 'UID': ''})


#default_table = tables[0]
#table = st.selectbox('Choose table', tables, tables.index(default_table))
#table_info = ds_info.tables[table]
#st.write(table_info.toDict())

#with ccols[1]:  # Table chooser dropdown

#    ingestion_default = ds_info.default
#    ingestions = ds_info.ingestions
#    ingestion = st.selectbox('Choose ingestion', ingestions, ingestions.index(ingestion_default))
#    dataurl = ds_info.url.replace('{x}', ingestion)
#    if 'columns' in ds_info:
#        selcols = ds_info.columns
#    file_info = get_file_info(dataset_name, dataurl, columns=selcols)
#with ccols[3]:
    # st.caption(f'Getting dataset {ingestion} from {dataurl}')
#    fi = file_info
#    schema = get_schema(dataset_name, fi.parquet, selcols)
#    coldict({'Name': schema.dataset.name, 'DataType': schema.dataset.datatype, 'UID': schema.dataset.uid})
#    coldict({f'{fi.pq_num_rows:,}': 'rows', fi.pq_num_cols: 'columns', 'On disk': f'{fi.csv_size:.1f} MB (raw)',
#             f'{fi.parquet_size:.1f}': 'MB (parquet)', f'{fi.mem_size:.1f}': 'MB (arrow)'}, 'Grey', 'Grey')

# Dataset and Table tabs
st.write('<style>div.row-widget.stRadio > div{flex-direction:row;} </style> ', unsafe_allow_html=True)
# tab = st.radio('', ['Dataset Info ___', 'Tables'])
tab1, tab2 = st.tabs(['Dataset Info', 'Tables'])

# if tab == 'Dataset Info ___':  # Show examples of tables and table relations
with tab1:
    for table in tables:
        table_file = get_table_file(dataset_name, table, ds_info.tables[table])
        dataset = ds.dataset(table_file, format="parquet")
        df = dataset.head(5).to_pandas()
        st.write(table)
        st.dataframe(df.style.applymap(dtypcol), height=200)
    a = 1
    # Show a dataframe of the first n rows
#    td = schema.model.table_data
#    coldict({'Table name': td.name, 'UID': td.uid})
#    # df = pt.slice(0, 20).to_pandas()
#    df = get_n_rows_to_df(file_info.parquet, 20)
#    st.dataframe(df.style.applymap(dtypcol), height=200)
#    # Make table constraints and transforms section
#    # make_table(row_data['fields'], pt)

# if tab == 'Tables':
with tab2:
    ccols = st.columns((2, 2, 1, 8))
    with ccols[0]:  # table chooser dropdown
        table = table_list[0]
        table_name = st.selectbox('Choose table', tables, table_list.index(table))
        table_info = tables[table_name]
    with ccols[1]:  # ingestion chooser dropdown
        ingestion_default = table_info.default_ingestion
        ingestions = table_info.ingestions
        ingestion = st.selectbox('Choose ingestion', ingestions, ingestions.index(ingestion_default))
    dataurl = table_info.url.replace('{x}', str(ingestion))
    if 'include_columns' in table_info:
        selcols = table_info.include_columns
    file_info = get_file_info(dataset_name, table_name, dataurl, columns=selcols)
    with ccols[3]:
        fi = file_info
        # mdi = metadata.model.tables[table].table_info
        # Get raw metadata from the parquet file
        row_data = get_row_data(file_info.parquet, file_info.columns)
        coldict({'Name': table_name, 'UpdateType': table_info['update'], 'NumRowGroups': row_data['num_row_groups'],
                 'UID': ''})
        coldict({f'{fi.pq_num_rows:,}': 'rows', fi.pq_num_cols: 'columns', 'On disk': f'{fi.csv_size:.1f} MB (raw)',
                 f'{fi.parquet_size:.1f}': 'MB (parquet)', f'{fi.mem_size:.1f}': 'MB (arrow)'}, 'Grey', 'Grey')
    st.markdown('---')
    # Get raw metadata from the parquet file
    row_data = get_row_data(file_info.parquet, file_info.columns)
    # Make the stats and schema sections for each field
    metadata = make_fields(row_data['fields'], file_info.parquet, metadata, table_name)

st.markdown('---')


# Validate and show schema
colbox('Schema', 'Green')
st.write("![ready](https://img.shields.io/static/v1?label=Status&message=Complete&color=005500)")
# scj = generate_schema(parquet_file)
if metadata is not None:
    dict_raw=metadata.dict()
    st.write(dict_raw)
    metadata_file = dataroot / dataset_name / 'metadata' / f'{dataset_name}.metadata.json'
    with metadata_file.open('w') as fh:
        fh.write(metadata.json(indent=2, exclude_unset=True))
    # check dict -> json -> file -> json -> dict
    import metadata_definition as md
    with metadata_file.open('r') as fh:  # Load the metadata
        test_metadata = json.load(fh)
        test_metadata = md.MetadataDefinition(**test_metadata)
        dict_jsonfile = test_metadata.dict()
        ddiff = DeepDiff(dict_raw, dict_jsonfile, ignore_order=True)
        st.write(f'metadata.dict() with metadata ➝ json ➝ file ➝ json ➝ metadata.dict()',  ddiff)
    # Write yaml
    metadata_file_yaml = metadata_file.with_suffix('.yaml')
    with metadata_file_yaml.open('w') as fh:
        fh.write(metadata.yaml(exclude_unset=True))
    with metadata_file_yaml.open('r') as fh:  # Load the metadata
        test_metadata_yaml = '\n'.join(fh.readlines())
        test_metadata_yaml = md.MetadataDefinition.parse_raw(test_metadata_yaml)
        dict_yamlfile = test_metadata_yaml.dict()
        ddiff = DeepDiff(dict_raw, dict_yamlfile, ignore_order=True)
        st.write(f'metadata.dict() with metadata ➝ yaml ➝ file ➝ yaml ➝ metadata.dict()',  ddiff)

    # validate schema
    # metaschema = json.load(open('schemas/caspian_metaschema.json'))
    # res = None
    # res = jsonschema.validate(schema, metaschema)
    # st.write('Schema validation : ', res)

st.caption('Where data becomes knowledge')
