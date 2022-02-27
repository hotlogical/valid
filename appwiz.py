import streamlit as st
from dtools import load_data, get_row_data, generate_schema
from stools import make_fields, colbox, make_table

# Must be first command executed
st.set_page_config(layout="wide")
st.title('Data description wizard')

# Define datasets and choose one
ccols = st.columns((1, 5))
selcols = None
with ccols[0]:  # dataset chooser dropdown
    datasets = 'yellow_taxi citibike taxi+_zone'.split()
    dataset = datasets[0]
    option = st.selectbox('Choose dataset', datasets, datasets.index(dataset))
    if option == 'yellow_taxi':
        ingestion = '2021-07'
        dataurl = f'https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_{ingestion}.csv'
        selcols = 'tpep_pickup_datetime tpep_dropoff_datetime passenger_count trip_distance store_and_fwd_flag PULocationID DOLocationID payment_type fare_amount'.split()
    elif option == 'citibike':
        ingestion = '202201'
        ingestion = '202107'
        dataurl = f'https://nyc-tlc.s3.amazonaws.com/trip+data/{ingestion}-citibike-tripdata.csv'
    else:
        ingestion = 'lookup'
        dataurl = f'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_{ingestion}.csv'
#with ccols[1]:  # Table of distributions
st.write(f'Getting ingestion {ingestion} from {dataurl}')

# Load the dataset
parquet_file, pt, rsize, pqsize, msize = load_data(dataurl, selcols)
ncols, nrows = pt.num_columns, pt.num_rows
st.write(f'{nrows:,} rows _ {ncols} columns ___ On disk _ {rsize:.1f} MB (raw) _ {pqsize:.1f} MB (parquet) ___ In memory {msize:.1f} MB (arrow)')

# Show a dataframe of the first n rows
df = pt.slice(0, 20).to_pandas()
st.dataframe(df, height=200)

# Get raw metadata from the parquet file
row_data = get_row_data(parquet_file, selcols)

# Make the stats and schema sections for each field
pr = make_fields(row_data['fields'], pt, parquet_file)

# Make table constraints and transforms section
make_table(pr, row_data['fields'], pt)

st.markdown('---')
# Validate and show schema
colbox('Schema', 'Black', 'Green')
scj = generate_schema(parquet_file)
if scj is not None:
    st.write(scj.dict())
    with open(f'schemas/{dataset}.schema.json', 'w') as f:
        f.write(scj.json(indent=2))

#df = pt.to_pandas()
#st.write(df.describe())
