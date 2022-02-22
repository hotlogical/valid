import prompt_toolkit.layout.screen
import streamlit as st
import numpy as np
import pandas as pd
import pyarrow.compute as pc
import generate_metaschema as gm
import proot
from dtools import setcolmetadata, readcolmetadata


dtypes = 'float double int32 int64 int96 byte_array timestamp'.split()
hide_table_row_index = "<style>tbody th {display:none}.blank {display:none}</style>"
hide_dataframe_row_index = "<style>.row_heading.level0 {display:none}.blank {display:none}</style>"

def colbox(text, back, fore=None):
    if fore is None:
        fore = back
    st.markdown(f'<p style="background-color:{back};color:{fore};width:30px"><b>{text}</b></p>', unsafe_allow_html=True)

def make_data_header(cols):
    with st.container():
        columns = st.columns(cols.values())
        for i, col in enumerate(cols.keys()):
            with columns[i]:
                st.text(col)

def make_numeric_stats(i, field_data, fa, pr):
    field = field_data['field_name']
    na = fa.to_numpy()
    min, mode, max = np.quantile(na, (0.001, 0.5, 0.999))
    min = float(round(min))
    max = float(round(max))
    nl = np.sort(na[na < min])[0:10]
    if len(nl) == 0:
        nl = np.array([min] * 10)
    nm = reversed(np.sort(na[na > max])[-10:])
    df = pd.DataFrame([nl,nm], 'min max'.split()).T
    # cols = st.columns((5, 2))
    cols = st.columns((1.5, 0.5, 5, 0.5))
    nbins = 40
    with cols[0]:  # Distribution graph
        bins = st.text_input('bins', nbins, 3)
        st.markdown(hide_dataframe_row_index, unsafe_allow_html=True)
        st.write('outliers')
        st.write(df)
    with cols[2]:  # Table of outliers
        mn, mx = st.slider('', min, max, (min, max), 1.)
        h = pr.rh(na, int(bins), mn, mx, t=f'{field};km', l='b', fi=proot.kBlue-7, out='st', o='')
        st.image('graph.png')  #, caption='Shit Biscuits')
        # components.iframe("http://127.0.0.1:8084/Canvases/c2/draw.htm", height=400)

def make_category_stats(i, field_data, vc, pr):
    field = field_data['field_name']
    dc = pd.DataFrame([vc.field(1).to_pylist(), vc.field(0).to_pylist()], 'counts values'.split()).T
    dc = dc.sort_values(['counts'], ascending=False)
    dc.insert(loc=0, column='index', value=[float(i) for i in range(len(vc))])
    dc.counts = dc.counts.astype('float64')
    dc['values'] = dc['values'].astype(str)
    dc['%'] = dc.counts / dc.counts.sum() * 100.
    dc['%'].round(1)
    cols = st.columns((5, 2))
    with cols[0]:  # Distribution graph
        h = pr.rh(dc, len(dc), 0., float(len(dc)), t=f'{field} (%)', l='W', fi=proot.kBlue-7, out='st', o='HIST', n=100)
        st.image('graph.png')  #, caption='Shit Biscuits')
        # components.iframe("http://127.0.0.1:8084/Canvases/c2/draw.htm", height=400)
    with cols[1]:  # Table of distributions
        st.markdown(hide_dataframe_row_index, unsafe_allow_html=True)
        st.write('value counts')
        dc.counts = dc.counts.astype('int64')
        st.write(dc[['values', 'counts', '%']])

def make_time_stats(i, field_data, fa, pr):
    field = field_data['field_name']
    df = fa.to_pandas()
    dd = df.describe(percentiles=[0.01, 0.25, 0.5, 0.77, 0.99], datetime_is_numeric=True)
    mind = dd.iloc[3].floor(freq='d')
    maxd = dd.iloc[7].ceil(freq='d')
    nbins = (maxd - mind).days
    mind = mind.to_pydatetime()
    maxd = maxd.to_pydatetime()
    cols = st.columns((1.5, 0.5, 5, 0.5))
    with cols[0]:
        bins = st.text_input('bins', nbins, 3)
        st.write(dd.iloc[1:])  # The num_counts as the first field creates mixed dtypes and confuses the streamlit df parser !?!?
    with cols[2]:  # Distribution graph
        mn, mx = st.slider('', mind, maxd, (mind, maxd), format='YYYY-MM-DD')
        min = pd.Timestamp(mn).value / 1000000000.0
        max = pd.Timestamp(mx).value / 1000000000.0
        ti = 1 if (mx - mn).days < 4 else 2
        h = pr.rh(df, int(bins), min, max, t=f'{field};date', l='b', fi=proot.kBlue-7, out='st', o='', ti=ti)
        st.image('graph.png')

def make_field_stats(i, field_data, pt, pr):
    field = field_data['field_name']
    fa = pt.column(i)
    # st.write('dtype ',field_data['dtype'])
    # st.write('logical ',field_data['logical'])
    if field_data['logical'].startswith('TIMESTAMP'):
        make_time_stats(i, field_data, fa, pr)
        return
    vc = pc.value_counts(fa)
    st.write(len(vc))
    if len(vc) < 500:
        make_category_stats(i, field_data, vc, pr)
    else:
        make_numeric_stats(i, field_data, fa, pr)

def make_names_section(i, field_data, pt, pr, section, fdict, parquet_file):
    field = field_data['field_name']
    fields = fdict[section]
    oldvalues = readcolmetadata(pt, i, section)
    if section == 'names':
        oldvalues['rawname'] = field
    if section == 'types':
        oldvalues['ftype'] = field_data['dtype']
        oldvalues['logicaltype'] = '' if field_data['logical'].lower() == 'none' else field_data['logical'].lower()
        oldvalues['arrowtype'] = field_data['arrowtype']
        oldvalues['representation'] = field_data['dtype']
    response = {}
    with st.form(f"form_{section}"):
        colbox(section, 'Black', 'Green')
        for f in fields:
            oldval = oldvalues[f] if f in oldvalues else ''
            if oldval is not '' and type(oldval) == str and len(oldval) > 40:
                response[f] = st.text_area(f, oldval, 150, None, f)
            else:
                if section in 'flags constraints'.split():
                    response[f] = st.checkbox(f, oldval, f)
                else:
                    response[f] = st.text_input(f, oldval, None, f)
        submitted = st.form_submit_button("Save")
        if submitted:
            st.write('Writing schema changes to ', parquet_file)
            pt = setcolmetadata(pt, i, section, response, parquet_file)

def make_schema_form(i, field_data, pt, pr, parquet_file):
    field = field_data['field_name']
    fdict = gm.field_dict()
    sections = fdict.keys()
    # section = st.radio('', fdict.keys())
    sections = 'names types flags constraints'.split()
    cols = st.columns((2., 2., 2., 2.))
    for k, section in enumerate(sections):
        with cols[k]:
            make_names_section(i, field_data, pt, pr, section, fdict, parquet_file)

def make_field_content(i, field_data, pt, pr, parquet_file):
    st.write('<style>div.row-widget.stRadio > div{flex-direction:row;} </style>',
             unsafe_allow_html=True)
    tab = st.radio('', ['Distribution', 'Statistics .', 'Names Types Flags  .', 'Constraints Transforms Standardizations'])
    if tab == 'Distribution':
        make_field_stats(i, field_data, pt, pr)
    if tab == 'Statistics .':
        df = pt.column(i).to_pandas().describe()
        st.write(df)
        # st.write('Statistics')
    if tab == 'Names Types Flags  .':
        #st.write('Schema')
        make_schema_form(i, field_data, pt, pr, parquet_file)
    st.markdown('---')

def make_field_row(r, field_data, cols, pt, pr, parquet_file):
    field = field_data['field_name']
    with st.container():
        columns = st.columns(cols.values())
        for i, col in enumerate(cols.keys()):
            with columns[i]:
                if col == 'field':
                    showfield = st.checkbox(field)
                elif col == 'status':
                    status = '#fb8b1e' if f'saved_{field}' in st.session_state else 'Green'
                    colbox('a', status)
                elif col == 'dtype':
                    typ = field_data['dtype']
                    st.text(typ)
                    #st.selectbox('', dtypes, dtypes.index(typ))
                elif col == 'logical':
                    typ = '' if field_data['logical'] == 'NONE' else field_data['logical'].lower()
                    st.text(typ)
                else:
                    st.text(field_data[col])
        if showfield:
            make_field_content(r, field_data, pt, pr, parquet_file)

def make_fields(field_data, pt, parquet_file):
    # Setup ROOT
    pr = proot.pROOT(True, True)
    # pr, serv = start_server()
    c2 = pr.createCanvas('c2', inline=True, width=800, height=800)

    # Format the headers for the field sections
    cols = 'field 0.8 status 0.2 dtype 0.4 logical 0.4 nulls 0.4 min 0.7 max 1.0'.split()
    cols = {cols[i]: float(cols[i + 1]) for i in range(0, len(cols), 2)}

    # Make the individual data field sections
    make_data_header(cols)
    for i, fd in enumerate(field_data):
        make_field_row(i, field_data[fd], cols, pt, pr, parquet_file)

