import boto3
import botocore

config = botocore.client.Config(
    region_name="me-jeddah-1",
    s3={"addressing_style": "path"}
)
s3 = boto3.client('s3',
    endpoint_url="https://axnfm4jb3i73.compat.objectstorage.me-jeddah-1.oraclecloud.com",
    aws_access_key_id="caa0b19072dfe3bfe75a4e47d2138fdf5d25e425",
    aws_secret_access_key="d80GFB+mcb51nF2Zn8Up/Yp0rN9BwA1RS3y7GX8Rp7U=",
    config=config,
    use_ssl=True)

list_obj = s3.list_objects(Bucket="marketplace-raw-data", Prefix="07f0f735-6d79-40de-a614-36898e483282/2cfb3873-d99c-4ab2-8029-695937309c59.csv")
for item in list_obj['Contents']:
    print(item)
