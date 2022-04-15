import datetime
import shutil
import json
import pyarrow.parquet as pq
from datatools import load_dataset_schema, get_datasets, get_hash, get_dataset_dirs
from validation import Validation
from UID import UID


class Ingestion(object):

    def __init__(self, dataset_name):
        self.dataset_name = dataset_name
        self.schema = load_dataset_schema(self.dataset_name)
        ds_info = get_datasets()
        self.dirs = get_dataset_dirs(dataset_name)
        self.ds_data = ds_info.datasets[dataset_name]
        self.ingestions = self.ds_data.ingestions  # [-2:]
        self.part = 10

    def copy_raw_data(self, ingestion):
        pq_filename = self.ds_data.pattern.replace('{x}', ingestion) + '.parquet'
        fields = self.schema.model.fields
        schema_cols = [fields[i].names.raw_name for i in range(len(fields))]
        pt = pq.read_table(self.dirs['raw_dir'] + pq_filename, schema_cols)
        self.working_file = self.dirs['dataset_dir'] + pq_filename
        pq.write_table(pt, self.working_file)

    def validate(self, ingestion):
        vd = Validation(self.dataset_name, self.schema)
        valid = vd.validate(self.working_file)
        validated = valid['validated']
        if validated < 2:
            return valid
        badjson = self.dirs['valid_dir'] + 'valid.json'
        with open(badjson, 'w') as fh:
            json.dump(valid, fh, indent=2)
        print(f'Ingestion {ingestion} fails validation')
        print(f'\tInspect validation output {badjson}')
        print('Exiting')
        exit(1)

    def ingest(self, ingestion):
        self.copy_raw_data(ingestion)
        valid = self.validate(ingestion)
        dataset_uid = self.schema.dataset.uid
        ingestion_uid = UID().str
        valid['ingestion'] = ingestion
        valid['uid'] = ingestion_uid
        out_base = f'{ingestion}_{dataset_uid}_{ingestion_uid}'
        out_parquet = f'{self.dirs["valid_dir"]}/part_{self.part:04}_{out_base}.parquet'
        self.part += 10
        shutil.copy(self.working_file, out_parquet)
        hash = get_hash(out_parquet)
        valid['sha256_hash'] = hash
        valid['parquet_file'] = out_parquet
        valid['ingest_time'] = str(datetime.datetime.now())
        out_json = f'{self.dirs["valid_dir"]}/valid_{out_base}.json'
        with open(out_json, 'w') as fh:
            json.dump(valid, fh, indent=2)


    def run_ingestions(self, ingestion=None):
        if ingestion is None:
            ingestion = self.ingestions
        if not isinstance(ingestion, list):
            ingestion = [ingestion]
        for i, ing in enumerate(ingestion):
            print(f'\nprocessing {i + 1} of {len(ingestion)} {ing}')
            self.ingest(ing)

if __name__ == '__main__':
    dataset_name = 'yellow_taxi'
    ingestion = '2021-06'
    ing = Ingestion(dataset_name)
    ing.run_ingestions()
