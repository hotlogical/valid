import pyarrow as pa
import pyarrow.parquet as pq
from datatools import setcolmetadata, readcolmetadata

# Tests for reading / writing metadata
fnam2 = 'data/yellow_tripdata_test2.parquet'
pt = pq.read_table(fnam2)
print(pt.metadata, '\n')
section = 'names'
fieldnum = 1
mdict = {'descriptionshort': 'Just a short description'}
pt = setcolmetadata(pt, fieldnum, section, mdict)
print(pt.metadata)
mdict = {'descriptionshort': 'Just another short description'}
pt = setcolmetadata(pt, fieldnum, section, mdict)
print(pt.metadata)
mdict = {'descriptionlong': 'A longer description (usually)'}
pt = setcolmetadata(pt, fieldnum, section, mdict)
print(pt.metadata)
print(pt.metadata.field(fieldnum).metadata)
print(readcolmetadata(pt, fieldnum))
