import json
import jsonschema
import streamlit as st
import pyarrow.dataset as ds
from datatools import get_datasets, get_metadata, get_file_info, parquet_from_yaml, get_table_file, \
    dataroot, get_dataset_info
from destools import make_fields, dtypcol, colbox, coldict, make_col_defs
from st_aggrid import AgGrid, GridOptionsBuilder
# from deepdiff import DeepDiff

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


# Dataset and Table tabs
st.write('<style>div.row-widget.stRadio > div{flex-direction:row;} </style> ', unsafe_allow_html=True)
# tab = st.radio('', ['Dataset Info ___', 'Tables'])
tab1, tab2 = st.tabs(['Dataset Info', 'Tables'])

with tab1:
    for table in tables:
        # with st.expander(table):
        table_file = get_table_file(dataset_name, table, ds_info.tables[table])
        dataset = ds.dataset(table_file, format="parquet")
        #df = dataset.head(5000).to_pandas()
        #with st.container():
        #    scols = st.columns((3, 4, 4,10))
        #    scols[0].write(table)
        #    scols[1].write('No. of rows')
        #    with scols[2]:
        #        show_rows = st.number_input('vcxvx', max_value=50000, value=50000, key=f'{table}_show_rows')
        # st.write(table)
        show_rows = 50000
        df = dataset.head(show_rows).to_pandas()
        # st.datafrme(df.style.applymap(dtypcol), height=200)
        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_side_bar()
        # gb.configure_default_column(groupabe=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
        gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum")
        gb.configure_pagination(paginationAutoPageSize=False)
        gb = make_col_defs(gb, metadata.dict()['directory'][table])
        gridOptions = gb.build()
        AgGrid(df, gridOptions=gridOptions, reload_data=False, fit_columns_on_grid_load=False)
        # st.write(gridOptions)

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
        # Get raw metadata from the parquet file
        pq_dataset = ds.dataset(file_info.parquet)
        nrg = list(pq_dataset.get_fragments())[0].metadata.num_row_groups
        st.write('nrg ', nrg)
        row_data = get_dataset_info(pq_dataset, file_info.columns).dict()
        coldict({'Name': table_name, 'UpdateType': table_info['update'], 'NumRowGroups': row_data['num_row_groups'],
                 'UID': ''})
        coldict({f'{fi.pq_num_rows:,}': 'rows', fi.pq_num_cols: 'columns', 'On disk': f'{fi.csv_size:.1f} MB (raw)',
                 f'{fi.parquet_size:.1f}': 'MB (parquet)', f'{fi.mem_size:.1f}': 'MB (arrow)'}, 'Grey', 'Grey')
    # st.markdown('---')
    # Make the stats and schema sections for each field
    metadata = make_fields(row_data['fields'], file_info.parquet, metadata, table_name)

    # Make table constraints and transforms section

# st.markdown('---')



# Validate and show schema
colbox('Schema', 'Green', width='100px')
# st.write("![ready](https://img.shields.io/static/v1?label=Status&message=Complete&color=005500)")

if metadata is not None:
    dict_raw=metadata.dict()
    # st.write(dict_raw)
    metadata_file = dataroot / dataset_name / 'metadata' / f'{dataset_name}.metadata.json'
    #with metadata_file.open('w') as fh:
    #    fh.write(metadata.json(indent=2, exclude_unset=True))
    ## check dict -> json -> file -> json -> dict
    #import metadata_definition as md
    #with metadata_file.open('r') as fh:  # Load the metadata
    #    test_metadata = json.load(fh)
    #    test_metadata = md.MetadataDefinition(**test_metadata)
    #    dict_jsonfile = test_metadata.dict()
    #    ddiff = DeepDiff(dict_raw, dict_jsonfile, ignore_order=True)
    #    st.write(f'metadata.dict() with metadata ➝ json ➝ file ➝ json ➝ metadata.dict()',  ddiff)
    # Write yaml
    metadata_file_yaml = metadata_file.with_suffix('.yaml')
    with metadata_file_yaml.open('w') as fh:
        fh.write(metadata.yaml(exclude_unset=True))
    #with metadata_file_yaml.open('r') as fh:  # Load the metadata
    #    test_metadata_yaml = '\n'.join(fh.readlines())
    #    test_metadata_yaml = md.MetadataDefinition.parse_raw(test_metadata_yaml)
    #    dict_yamlfile = test_metadata_yaml.dict()
    #    ddiff = DeepDiff(dict_raw, dict_yamlfile, ignore_order=True)
    #    st.write(f'metadata.dict() with metadata ➝ yaml ➝ file ➝ yaml ➝ metadata.dict()',  ddiff)

    # validate schema
    # metaschema = json.load(open('schemas/caspian_metaschema.json'))
    # res = None
    # res = jsonschema.validate(schema, metaschema)
    # st.write('Schema validation : ', res)

st.caption('Where data becomes knowledge')

import pandas as pd
from holoviews.element.tiles import EsriImagery
import holoviews as hv
import colorcet as cc
from datashader.utils import lnglat_to_meters

# df = pd.read_csv('/home/elc/data/techsalerator_poi/raw/middleeast_poi.csv')
df = pd.read_csv('/home/elc/repos/valid/testoutsep.csv')
dfr = df[df.brandID.isin([19, 20, 21])]
def get_xy(dflon, dflat):
    return pd.DataFrame(lnglat_to_meters(dflon, dflat), index='x y'.split()).T
dfp = get_xy(dfr.Longitude, dfr.Latitude)

from bokeh.themes.theme import Theme
theme = Theme(
    json={
    'attrs' : {
        'Figure' : {
            'background_fill_color': '#000000',
            'border_fill_color': '#000000',
            'outline_line_color': '#000000',
        },
        'Grid': {
            'grid_line_dash': [6, 4],
            'grid_line_alpha': .3,
        },

        'Axis': {
            'major_label_text_color': 'blue',
            'axis_label_text_color': 'green',
            'major_tick_line_color': 'red',
            'minor_tick_line_color': 'white',
            'axis_line_color': "yellow"
        }
    }
})


map_tiles = EsriImagery().opts(alpha=0.9, width=900, height=600, bgcolor='black')
plot = dfp.hvplot(
    'x',
    'y',
    kind='scatter',
    rasterize=False,
    cmap=cc.fire,
    cnorm='eq_hist',
    colorbar=False)  # .opts(colorbar_position='bottom')
fig = hv.render(map_tiles * plot, backend='bokeh', theme='night_sky')  # 'dark_minimal'
fig.border_fill_color = '#0e1117'
st.bokeh_chart(fig)

