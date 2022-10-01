from fastapi import FastAPI
import sys
sys.path.append('/home/elc/repos/data-products/lib')
from baku import configure
#env = configure(filename='baku.conf.yaml')
env = configure()
print(env)
vtx = env.fetch('caspian/tlc_yellowtaxi', readfrom='shared')
print(vtx)
arts = vtx.list_artifacts()
print(arts)

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}

#