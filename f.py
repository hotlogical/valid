from datetime import datetime
from faker import Faker
import pandas as pd

# Make fake names
a = datetime.now()

#fake = Faker(use_weighting=False)
fake = Faker()
n = 3000000

names = [fake.name() for _ in range(n)]
b = datetime.now() - a
print('names ', names[:10])
print(n, b)
df = pd.DataFrame(names)
#df = df.T
df.columns=['names']
print('pandas names')
print(df.head())
df.to_parquet('data/names.parquet')
df2 = pd.read_parquet('data/names.parquet')
print('parquet names')
print(df2.head())

