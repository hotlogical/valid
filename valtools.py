import streamlit as st
import pandas as pd
import numpy as np
import proot
from dotmap import DotMap
from destools import colbox, rgraph

col_map = {0: 'Green', 1: 'Orange', 2: 'Red'}

# Setup ROOT
pr = proot.pROOT(True, False)
c2 = pr.createCanvas('c2', inline=True, width=800, height=800)


def validate_table(valid):
    validate_column_names(valid)
    validate_rows(valid)


def make_num_row_graph(rows, vs):
    # na = rows.to_numpy().astype(np.float64)
    na = np.array(rows).astype(np.float64)
    g, json = pr.rg(na, t=f'num Rows;ingestion', out='st')
    rgraph(json)


def validate_column_names(valid):
    table = valid['table']
    num_data_cols = table['num_data_cols']
    num_schema_cols = table['num_schema_cols']
    checks = table['column_name_checks']
    match list(checks.keys())[0]:
        case 'columns_validated':
            st.write('Schema and ingestion column names are identical')
            # st.write(table['data_cols'])
        case 'missing_columns':
            st.write(f'Error: ingested table has {num_schema_cols - num_data_cols} missing columns...')
            st.write(f'... {checks["missing_columns"]}')
        case 'extra_columns':
            st.write(f'Warning: ingested table has {num_data_cols - num_schema_cols} extra columns...')
            st.write(f'... {checks["extra_columns"]}')
            st.write('... Ignoring extra columns')
        case 'different_columns':
            st.write('Error: Ingested table has different column names')
            df = pd.DataFrame([table["schema_cols"], table["data_cols"]]).T
            df.columns = 'schema data'.split()
            st.write(df)
        case 'column_order':
            st.write('Warning: Schema and ingestion column names are in different order')
            df = pd.DataFrame([table["schema_cols"], table["data_cols"]]).T
            df.columns = 'schema data'.split()
            st.write(df)

def make_columns_header(cols):
    # Header row for the data fields table
    with st.container():
        columns = st.columns(cols.values())
        for i, col in enumerate(cols.keys()):
            with columns[i]:
                st.text(col)


def close_other_fields(fkey, my_key, all_columns):
    # If we open a new field detail - close any open fields
    for c in all_columns:
        thisfkey = f'{my_key}_{c}'
        if thisfkey in st.session_state:
            if thisfkey != fkey:
                st.session_state[thisfkey] = False

def constraints_to_df(constraints):
    dfs = []
    for c in constraints:
        df = pd.DataFrame([dict(c)])
        dfs.append(df)
    df = pd.concat(dfs)
    df1 = df.where(pd.notnull(df), None)
    return df1

def make_column_content(col_name, check, schema):
    # st.write('<style>div.row-widget.stRadio > div{flex-direction:row;} </style>',
    #          unsafe_allow_html=True)
    # tab = st.radio('', ['Constraints', 'Comparison'])
    # if tab == 'Constraints':  # Show graph of data and some stats
    constraints = check.constraints
    st.write(constraints_to_df(constraints))
    #for c in constraints:
    #    st.write(dict(c))
    # if tab == 'Comparison':  # Further stats
    #     pass
    st.markdown('---')
    return



def make_column_row(col_name, checks, cols, schema):
    # First the row containing the parquet metadata
    all_columns = list(checks.keys())
    check = checks[col_name]
    my_key = 'column'
    with st.container():
        columns = st.columns(cols.values())
        for i, col in enumerate(cols.keys()):
            with columns[i]:
                match col:
                    case 'column':
                        fkey = f'{my_key}_{col_name}'
                        showfield = st.checkbox(col_name, key=fkey, on_change=close_other_fields, args=(fkey, my_key, all_columns))
                    case 'status':
                        status = col_map[check.column_level]
                        colbox('.', status, status)
                    case'dtype':
                        typ = schema.types.pq_type
                        st.text(typ)
                        # st.selectbox('', dtypes, dtypes.index(typ))
                    case 'logical':
                        typ = schema.types.logical_type
                        st.text('' if typ is None else typ)
                    case 'nulls':
                        if 'null_entries' in check:
                            st.text(f'{check.null_entries.percent:.2f} %')
                    case 'constraints':
                        st.text('')
                    case _:
                        st.text('')
        if showfield:
            # Then the details
            make_column_content(col_name, check, schema)
    return


def make_columns(valid, schema):
    # Main loop to generate each field

    # Format the headers for the field sections
    cols = 'column 0.8 status 0.2 dtype 0.4 logical 0.4 nulls 0.4 constraints 0.7 . 1.0'.split()
    cols = {cols[i]: float(cols[i + 1]) for i in range(0, len(cols), 2)}

    # Make the individual data field sections
    make_columns_header(cols)
    checks = valid.table.column_checks
    for i, col_name in enumerate(checks):
        make_column_row(col_name, checks, cols, schema.model.fields[i])


def make_ingestions_row(ingestion, validations, cols, schema):
    # First the row containing the parquet metadata
    all_ingestions = list(validations.keys())
    validation = validations[ingestion]
    my_key = 'ingest'
    with st.container():
        columns = st.columns(cols.values())
        for i, col in enumerate(cols.keys()):
            with columns[i]:
                match col:
                    case 'ingestion':
                        fkey = f'{my_key}_{ingestion}'
                        showfield = st.checkbox(ingestion, key=fkey, on_change=close_other_fields, args=(fkey, my_key, all_ingestions))
                    case 'status':
                        status = col_map[validation['validated']]
                        colbox('.', status, status)
                    case 'num_cols':
                        typ = validation['table']['num_data_cols']
                        st.text(typ)
                    case 'num_rows':
                        typ = validation['table']['num_rows']
                        st.text(f'{typ:,}')
                    case 'uid':
                        typ = validation['uid']
                        st.text('' if typ is None else typ)
                    case 'ingestion_time':
                        typ = validation['ingestion_time']
                        st.text(typ)
                    case _:
                        st.text('')
        if showfield:
            make_columns(DotMap(validation), schema)
    return


def make_ingestions(validations, schema):
    cols = 'ingestion 0.8 status 0.2 num_rows 0.4 uid 0.4 nulls 0.4 datetime 0.7 . 1.0'.split()
    cols = {cols[i]: float(cols[i + 1]) for i in range(0, len(cols), 2)}

    # Make the individual data field sections
    make_columns_header(cols)
    for ingestion in validations:
        make_ingestions_row(ingestion, validations, cols, schema)


def validate_rows(valid):
    pass


def validate_columns():
    pass
