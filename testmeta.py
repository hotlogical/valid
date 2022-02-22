import pyarrow as pa
import pyarrow.parquet as pq
from dtools import setcolmetadata, readcolmetadata

fnam2 = 'data/yellow_tripdata_test2.parquet'
pt = pq.read_table(fnam2)
print(pt.schema, '\n')
section = 'names'
fieldnum = 1
mdict = {'descriptionshort': 'Just a short description'}
pt = setcolmetadata(pt, fieldnum, section, mdict)
print(pt.schema)
mdict = {'descriptionshort': 'Just another short description'}
pt = setcolmetadata(pt, fieldnum, section, mdict)
print(pt.schema)
mdict = {'descriptionlong': 'A longer description (usually)'}
pt = setcolmetadata(pt, fieldnum, section, mdict)
print(pt.schema)
print(pt.schema.field(fieldnum).metadata)
print(readcolmetadata(pt, fieldnum))
