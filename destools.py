import streamlit as st
import numpy as np
import pandas as pd
import pyarrow.compute as pc
import metadata_definition as gm
# import proot
from datatools import get_constraints, type_constraints, read_parquet
import streamlit.components.v1 as components
import plotly.express as px
from fast_histogram import histogram1d
from st_aggrid import AgGrid, GridOptionsBuilder

# Setup ROOT
# pr = proot.pROOT(True, True)
# c2 = pr.createCanvas('c2', inline=True, width=800, height=800)

sections = {'names': gm.ColumnNames, 'types': gm.ColumnTypes, 'flags': gm.ColumnFlags}
dtypes = 'float double int32 int64 int96 byte_array timestamp'.split()
hide_table_row_index = "<style>tbody th {display:none}.blank {display:none}</style>"
hide_dataframe_row_index = "<style>.row_heading.level0 {display:none}.blank {display:none}</style>"
all_constraints = get_constraints()
col_map = {'int64': {'background-color': '#002200'},
           'double': {'background-color': '#000044'},
           'timestamp': {'background-color': '#330000'},
           'date': {'background-color': '#330000'},
           'time': {'background-color': '#330000'}}

def dtypcol(cval):
    cmap = {'int': 'background-color:#002200',
           'float': 'background-color:#000044',
           'Timestamp': 'background-color:#330000',
           'date': 'background-color:#330000',
           'time': 'background-color:#330000'}
    dtyp = type(cval).__name__
    # print(dtyp)
    if dtyp in cmap:
        return cmap[dtyp]
    return ''

def make_col_defs(gb, tabledata):
    for column in tabledata['columns']:
        names = column.get('names', None)
        if names is None:
            continue
        typs = column.get('types', None)
        if typs is None:
            continue
        name = names['raw_name']
        typ = typs['logical_type']
        if typ in col_map:
            if typ == 'timestamp':
                gb.configure_column(name, cellStyle=col_map[typ], type=['dateColumnFilter', 'customDateTimeFormat'],
                                    custom_format_string='yyyyMMdd  HH:mm')
            else:
                gb.configure_column(name, cellStyle=col_map[typ])
    return gb


def vspace(n):
    for i in range(n):
        st.write('')


def coltext(text, fore, back='Black', typ='div', inline=True, width=None):
    inl = 'display:inline;' if inline else ''
    wid = f'width:{width};' if width is not None else ''
    return f'<{typ} style="{inl}{wid}background-color:{back};color:{fore}"><b>{text}</b></{typ}>'


def colspace(n=3, col='Black'):
    text = '_' * n
    return coltext(text, col, col)


def coldict(vals, col1='Green', col2='White', space=3, back='Black'):
    out = ''
    for k, v in vals.items():
        out += coltext(f'{k}: ', col1, back) + coltext(v, col2, back) + colspace(space, back)
    st.markdown(out, unsafe_allow_html=True)


def colbox(text, fore, back='Black', typ='p', inline=False, width='100px'):
    st.markdown(coltext(text, fore, back, typ, inline, width), unsafe_allow_html=True)


def rgraph(js):
    components.html(f'''
        <div id="drawing" ></div>
        <script type="text/javascript" src="https://root.cern/js/latest/scripts/JSRoot.core.js"></script>
        <script type='text/javascript'>
            JSROOT.draw("drawing", JSROOT.parse({js}));
        </script>''', width=800, height=400)


def make_data_header(cols):
    # Header row for the data fields table
    with st.container():
        columns = st.columns(cols.values())
        for i, col in enumerate(cols.keys()):
            with columns[i]:
                st.text(col)


def make_numeric_stats(field_data, fa):
    # Graph and stats for numeric fields
    field = field_data['field_name']
    na = fa.to_numpy() # .astype(np.float64)
    mmin, mode, mmax = np.quantile(na, (0.001, 0.5, 0.999))
    mmin = float(round(mmin))
    mmax = float(round(mmax))
    nl = np.sort(na[na < mmin])[0:10]
    if len(nl) == 0:
        nl = np.array([mmin] * 10)
    nm = reversed(np.sort(na[na >= mmax])[-10:])
    df = pd.DataFrame([nl, nm], 'min max'.split()).T
    # cols = st.columns((5, 2))
    cols = st.columns((1.5, 0.5, 5, 0.5))
    nbins = 40
    with cols[0]:  # Distribution graph
        bins = st.text_input('bins', nbins, 3)
        st.markdown(hide_dataframe_row_index, unsafe_allow_html=True)
        st.write('outliers')
        st.write(df)
    with cols[2]:  # Table of outliers
        mn, mx = st.slider('', mmin, mmax, (mmin, mmax), 1.)
        # h, json = pr.rh(na, int(bins), mn, mx, t=f'{field};km', l='b', fi=proot.kBlue-7, out='st', o='')
        # rgraph(json)

        values = histogram1d(na, bins=int(bins), range=(mn, mx))
        halfbin = 0.5 * ((mx - mn) / float(bins))
        bincenters = np.linspace(mn + halfbin, mx - halfbin, int(bins))
        fig = px.bar(x=bincenters, y=values, labels={'x': field, 'y': 'count'})
        fig.update_layout(bargap=0.0)
        fig.data[0]["marker"]['line']["width"] = 0
        st.plotly_chart(fig, use_container_width=True)


def make_category_stats(field_data, vc, numeric):
    # Graph and stats for categorical data
    field = field_data['field_name']
    dc = pd.DataFrame([vc.field(1).to_pylist(), vc.field(0).to_pylist()], 'counts values'.split()).T
    if numeric:
        dc = dc.sort_values(['values'])
    else:
        dc = dc.sort_values(['counts'], ascending=False)
    dc.insert(loc=0, column='index', value=[float(i) for i in range(len(vc))])
    dc.counts = dc.counts.astype('float64')
    dc['values'] = dc['values'].astype(str)
    dc['%'] = dc.counts / dc.counts.sum() * 100.
    dc['%'].round(1)
    cols = st.columns((5, 2))
    with cols[0]:  # Distribution graph
        # h, js = pr.rh(dc, len(dc), 0., float(len(dc)), t=f'{field} (%)', l='W', fi=proot.kBlue-7, out='st', o='hist', n=100)
        # rgraph(js)
        fig = px.bar(x=dc['values'], y=dc['counts'], labels={'x': field, 'y': 'count'})
        fig.update_layout(bargap=0.0)
        fig.data[0]["marker"]['line']["width"] = 0
        st.plotly_chart(fig, use_container_width=True)


    with cols[1]:  # Table of value counts
        st.markdown(hide_dataframe_row_index, unsafe_allow_html=True)
        st.write('value counts')
        dc.counts = dc.counts.astype('int64')
        dc = dc.sort_values(['counts'], ascending=False)
        st.write(dc[['values', 'counts', '%']])


def make_time_stats(field_data, fa):
    # Special treatment for datetime fields
    field = field_data['field_name']
    df = fa.to_pandas()
    dd = df.describe(percentiles=[0.01, 0.25, 0.5, 0.77, 0.99], datetime_is_numeric=True)  # TODO convert this to arrow
    mind = dd.iloc[3].floor(freq='d')
    maxd = dd.iloc[7].ceil(freq='d')
    nbins = (maxd - mind).days
    mind = mind.to_pydatetime()
    maxd = maxd.to_pydatetime()
    cols = st.columns((1.5, 0.5, 5, 0.5))
    with cols[0]:  # No. bins chooser and stats
        bins = st.text_input('bins', nbins, 3)
        st.write(dd.iloc[1:])
        # The num_counts as the first field creates mixed dtypes and confuses the streamlit df parser !?!?
    with cols[2]:  # Distribution graph
        mn, mx = st.slider('', mind, maxd, (mind, maxd), format='YYYY-MM-DD')
        # mmin = pd.Timestamp(mn).value / 1000000000.0
        # mmax = pd.Timestamp(mx).value / 1000000000.0
        # ti = 1 if (mx - mn).days < 4 else 2
        # # pr.st.SetOptStat('n')
        #h, js = pr.rh(df, int(bins), mmin, mmax, t=f'{field};date', l='b', fi=proot.kBlue-7, out='st', o='bar1', ti=ti)
        #rgraph(js)

        na = fa.to_numpy().astype(np.int64)
        thismn = np.datetime64(mn).astype(np.int64) // 1000
        thismx = np.datetime64(mx).astype(np.int64) // 1000
        values = histogram1d(na, bins=int(bins), range=(thismn, thismx))
        binedges = np.linspace(thismn, thismx, int(bins))
        dates = pd.to_datetime(binedges, unit='ms')
        fig = px.bar(x=dates, y=values, labels={'x': field, 'y': 'count'})
        fig.update_layout(bargap=0.0)
        fig.data[0]["marker"]['line']["width"] = 0
        st.plotly_chart(fig, use_container_width=True)


def make_field_stats(i, field_data, pt, metadata, table_name):
    # Make different stats depending on field type
    fa = pt.column(0)
    if field_data['logical'].startswith('TIMESTAMP'):
        make_time_stats(field_data, fa)  # Special for datetimes
        return
    vc = pc.value_counts(fa)
    mdmt = metadata.directory[table_name]
    cat = mdmt.columns[i].flags.is_categorical
    numeric = mdmt.columns[i].flags.is_numeric
    if cat is None and len(vc) < 30:
        cat = True
        mdmt.columns[i].flags.is_categorical = True
    # if len(vc) < 500:
    if cat:
        make_category_stats(field_data, vc, numeric)  # Categorical data
    else:
        if numeric:
            make_numeric_stats(field_data, fa)  # Numeric data
        else:
            make_category_stats(field_data, vc, numeric)

def make_names_section(i, section, fdict, parquet_file, metadata, table_name):
    fields = fdict[section]
    # oldvalues = readcolmetadata(pt, i, section)
    mdmt = metadata.directory[table_name]
    oldvalues = getattr(mdmt.columns[i], section).dict()
    response = {}
    with st.form(f"form_{section}"):
        colbox(section, 'Green')
        for f in fields:
            oldval = oldvalues[f] if f in oldvalues else ''
            if oldval != '' and type(oldval) == str and len(oldval) > 40:
                response[f] = st.text_area(f, oldval, 150, None, f)
            else:
                if section in 'flags constraints'.split():
                    response[f] = st.checkbox(f, oldval, f)
                else:
                    oldval = '' if oldval is None else oldval
                    response[f] = st.text_input(f, oldval, None, f)
                    if response[f] == '':
                        response[f] = None
        submitted = st.form_submit_button("Save")
        if submitted:
            st.write('Writing schema changes to ', parquet_file)
            setattr(mdmt.columns[i], section, sections[section](**response))
            # pt = setcolmetadata(pt, i, section, response, parquet_file)
        return metadata

def make_names_types_flags_form(i, parquet_file, metadata, table_name):
    fdict = gm.field_dict()
    form_sections = 'names types flags'.split()
    cols = st.columns((2., 2., 2.))
    for k, section in enumerate(form_sections):
        with cols[k]:
            metadata = make_names_section(i, section, fdict, parquet_file, metadata, table_name)
    return metadata

def make_constraints_form(n, field_data, parquet_file, metadata, table_name):
    field = field_data['field_name']
    mdmt = metadata.directory[table_name]
    clist = type_constraints(mdmt.columns[n].types.parquet_type, all_constraints)
    cols = 'constraint 2 warning 2 error 2 enabled 1 delete 1 . 1'.split()
    cols = {cols[i]: float(cols[i + 1]) for i in range(0, len(cols), 2)}
    oldvalues = mdmt.columns[n].constraints
    cons = [] if oldvalues is None else [c.dict() for c in oldvalues]

    colbox('Constraints', 'Green', width='100px')
    addconstraint = st.button("Add Constraint")
    if addconstraint:
        cons.append({'name': 'greater_equal', 'values': {'warning': '', 'error': ''}, 'enabled': False})

    newcons = []
    newlist = []
    numdel = 0
    with st.form(f"form_constraints_{field}", clear_on_submit=False):
        if len(cons) == 0:
            colbox('No_constraints', 'Yellow')
        else:  # Draw headers
            columns = st.columns(cols.values())
            for i, col in enumerate(cols.keys()):
                with columns[i]:
                    st.text(col)
        for i, c in enumerate(cons):
            if f'cons_{i}_4' in st.session_state:  # ugh, Don't display deleted constraint
                if st.session_state[f'cons_{i}_4']:
                    continue
            columns = st.columns(cols.values())
            thiscons = {}
            with columns[0]:
                consind = clist.index(cons[i]['name'])
                thiscons['name'] = st.selectbox('', clist, consind, key=f'cons_{i}_0')
            thiscons['values'] = {}
            with columns[1]:
                thiscons['values']['warning'] = st.text_input('', cons[i]['values']['warning'], key=f'cons_{i}_1')
            with columns[2]:
                thiscons['values']['error'] = st.text_input('', cons[i]['values']['error'], key=f'cons_{i}_2')
            with columns[3]:
                vspace(3)
                thiscons['enabled'] = st.checkbox('', cons[i]['enabled'], f'cons_{i}_3')
            with columns[4]:
                vspace(3)
                delete = st.checkbox('', False, f'cons_{i}_4')
            if not delete:
                print(thiscons)
                con = gm.Constraint(**thiscons)
                print(con)
                newcons.append(thiscons)
                newlist.append(con)
            else:
                numdel += 1
        submitted = st.form_submit_button("Save")
        if addconstraint or submitted:
            mdmt.columns[n].constraints = [gm.Constraint(**c) for c in newcons]
            st.write('Writing schema changes to ', parquet_file)
    return metadata


def make_field_content(i, field_data, parquet_file, metadata, table_name):
    pt = read_parquet(parquet_file, [field_data['field_name']])
    st.write('<style>div.row-widget.stRadio > div{flex-direction:row;} </style>',
             unsafe_allow_html=True)
    tab1, tab2, tab3, tab4, tab5 = st.tabs(['Distribution', 'Statistics', 'Names Types Flags',
                                            'Constraints', 'Transforms / Standardizations'])
    with tab1:
        make_field_stats(i, field_data, pt, metadata, table_name)
    with tab2:
        df = pt.column(0).to_pandas().describe(include='all')
        # st.write(df)  # TODO FIX
    with tab3:
        metadata = make_names_types_flags_form(i, parquet_file, metadata, table_name)
    with tab4:
        metadata = make_constraints_form(i, field_data, parquet_file, metadata, table_name)
    with tab5:
        pass
    return metadata


def close_other_fields(fkey, field_data):
    # If we open a new field detail - close any open fields
    for f in field_data:
        field = field_data[f]['field_name']
        thisfkey = f'render_{field}'
        if thisfkey in st.session_state:
            if thisfkey != fkey:
                st.session_state[thisfkey] = False


def make_field_row(r, field_data, cols, parquet_file, metadata, table_name):
    # First the row containing the parquet metadata
    allfielddata = field_data
    field_data = field_data[list(field_data.keys())[r]]
    field = field_data['field_name']
    with st.container():
        columns = st.columns(cols.values())
        for i, col in enumerate(cols.keys()):
            with columns[i]:
                if col == 'field':
                    fkey = f'render_{field}'
                    showfield = st.checkbox(field, key=fkey, on_change=close_other_fields, args=(fkey, allfielddata))
                elif col == 'status':
                    status = '#fb8b1e' if f'saved_{field}' in st.session_state else 'Green'
                    colbox('.', status, status)
                elif col == 'dtype':
                    typ = field_data['dtype']
                    st.text(typ)
                    # st.selectbox('', dtypes, dtypes.index(typ))
                elif col == 'logical':
                    typ = '' if field_data['logical'] == 'NONE' else field_data['logical'].lower()
                    st.text(typ)
                else:
                    st.text(field_data[col])
        if showfield:
            # Then the details
            metadata = make_field_content(r, field_data, parquet_file, metadata, table_name)
    return metadata


def make_fields(field_data, parquet_file, metadata, table_name):
    # Main loop to generate each field

    # Format the headers for the field sections
    cols = 'field 0.8 status 0.2 dtype 0.4 logical 0.4 nulls 0.4 min 0.7 max 1.0'.split()
    cols = {cols[i]: float(cols[i + 1]) for i in range(0, len(cols), 2)}
    dff = pd.DataFrame(field_data).T
    dff = dff['field_name dtype logical nulls min max'.split()]
    dff['logical'] = dff.logical.str.lower()
    gb = GridOptionsBuilder.from_dataframe(dff)
    gb.configure_pagination(paginationAutoPageSize=False)
    gb.configure_default_column(enablePivot=True, enableValue=True, enableRowGroup=True)
    gb.configure_selection('multiple', use_checkbox=True,
                           groupSelectsChildren="Group checkbox select children")  # Enable multi-row selection
    gridOptions = gb.build()
    grid_response = AgGrid(dff, gridOptions=gridOptions, data_return_mode='AS_INPUT', update_mode='MODEL_CHANGED',
                           fit_columns_on_grid_load=False, theme='streamlit', enable_enterprise_modules=True,
                           reload_data=True)
    data = grid_response['data']
    selected = grid_response['selected_rows']
    # st.write(data)

    if len(selected) == 1:
        field = selected[0]['field_name']
        r = selected[0]['_selectedRowNodeInfo']['nodeRowIndex']
        # st.write(r, field)
        metadata = make_field_content(r, field_data[list(field_data.keys())[r]], parquet_file, metadata, table_name)

    if len(selected) == 2:
        st.write(selected[0]['field_name'], selected[1]['field_name'])

    # Make the individual data field sections
    #make_data_header(cols)
    #for i, fd in enumerate(field_data):
    #     metadata = make_field_row(i, field_data, cols, parquet_file, metadata, table_name)
    return metadata


def make_table(field_data, pt):
    cols = st.columns((1, 4))
    fnums = {field_data[f]['field_name']: i for i, f in enumerate(field_data)}
    fields = list(fnums.keys())
    with cols[0]:
        fx = st.selectbox('x', fields, 0, key='x')
        nx = st.number_input('', 1, 500, 30, key='nx')
        fy = st.selectbox('y', fields, 1, key='y')
        ny = st.number_input('', 1, 500, 30, key='ny')
    with cols[1]:
        a = pt.column(0).combine_chunks()
        b = pt.column(1).combine_chunks()
        b = pc.subtract(b, a).to_numpy().astype(np.float64)
        dx = pt.column(fnums[fx]).to_numpy().astype(np.float64)
        dy = pt.column(fnums[fy]).to_numpy().astype(np.float64)
        dy = dy - dx
        mnx, mxx = np.quantile(dx, (0.001, 0.99))
        mny, mxy = np.quantile(b, (0.001, 0.995))
        mxx = 2.
        mny = 0.
        mxy = 200000.
        # h, js = pr.rh2(dx, b, nx, ny, mnx, mxx, mny, mxy, out='st', t=f';{fx};{fy}', o='cont1')
        # #h, js = pr.rh(b, ny, mny, mxy, out='st', t=f';{fx};{fy}', l='b', fi=proot.kBlue - 7, o='c')
        # rgraph(js)
    st.write(mny, mxy, type(b))
