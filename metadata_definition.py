from typing import List, Dict, Optional, Union, Any, Callable
from pydantic import BaseModel
# import json
import jsonschema

# Generate Caspian Meta-Schema

class ColumnNames(BaseModel):
    # Names section of DataField
    uid: str
    raw_name: str
    display_name: Optional[str]
    description_short: Optional[str]
    description_long: Optional[str]

class ColumnTypes(BaseModel):
    # Types section of DataField
    parquet_type: str
    logical_type: Optional[str]
    arrow_type: Optional[str]
    representation: Optional[str]
    units: Optional[str]

class ColumnFlags(BaseModel):
    # Flags section of DataField
    is_raw: Optional[bool]
    is_pii: Optional[bool]
    is_standardised: Optional[bool]
    is_transformed: Optional[bool]
    is_inherited: Optional[bool]
    is_join: Optional[bool]
    is_key: Optional[bool]
    is_synthetic: Optional[bool]
    is_backfilled: Optional[bool]
    is_categorical: Optional[bool]
    is_numeric: Optional[bool]

class Transforms(BaseModel):
    # Transform definitions
    str_to_date: Optional[Callable]
    miles_to_km: Optional[Callable]
    latlon_to_H3: Optional[Callable]

class ColumnTransforms(BaseModel):
    # Transforms section of DataColumn - a list of Transforms
    transforms: Optional[List[Transforms]]

class Constraint(BaseModel):
    # Constraints section of DataField
    name: str
    values: Dict[str, Any]
    enabled: bool

class Status(BaseModel):
    status: str

class DataColumn(BaseModel):
    status: Optional[Status]
    names: ColumnNames
    types: ColumnTypes
    flags: Optional[ColumnFlags]
    # transforms: Optional[FieldTransforms]
    constraints: Optional[List[Constraint]]

class DecodeInfo(BaseModel):
    file_type: str
    delimiter: str = ','
    null_values: Optional[List[str]]
    column_names: Optional[List[str]]
    include_columns: Optional[List[str]]
    column_types: Optional[Dict[str, str]]
    timestamp_parsers: Optional[List[str]]
    safe: str = True

class TableInfo(BaseModel):
    # Table metadata
    name: str
    uid: str
    decodeinfo: Optional[DecodeInfo]

class DataSetData(BaseModel):
    name: str
    datatype: str
    uid: str

class DataTable(BaseModel):
    status: Optional[Status]
    table_info: Optional[TableInfo]
    columns: List[DataColumn]
    table_constraints: Optional[List[Constraint]]

class TableColumn(BaseModel):
    table: str
    column: str

class TableRelations(BaseModel):
    values: Dict[TableColumn, TableColumn]

class Tabular(BaseModel):
    status: Optional[Status]
    tables: Dict[str, DataTable]
    relations: Optional[List[TableRelations]]

class Graph(BaseModel):
    name: str

class Nested(BaseModel):
    name: str

class MetadataDefinition(BaseModel):
    """
    This is the Caspian meta-schema
    """
    class Config:
        schema_extra = {
            '$schema': 'https://json-schema.org/draft/2019-09/schema'
        }
    dataset: DataSetData
    model: Union[Tabular, Graph, Nested]


def field_dict():
    # Utility function to return a dict of sub-fields of a DataField
    fdict = {}
    subfields = []
    dfields = DataColumn.__fields__
    # print('dfields ', dfields)
    for fieldgroup in dfields:
        field = dfields[fieldgroup]
        # print('\tfield blob', field)
        typ = field.type_
        subfields = list(typ.__fields__.keys())
        # print('\tsubfields', subfields)
        fdict[fieldgroup] = subfields
    return fdict

def write_metaschema(fnam):
    # Write the metaschema as JSON
    jsonout = MetadataDefinition.schema_json(indent=2)
    print('schema = ', jsonout)
    with open(fnam, 'w') as f:
        f.write(jsonout)

if __name__ == '__main__':
    write_metaschema('schemas/caspian_metaschema.json')
    schema_obj = MetadataDefinition.schema()
    # print(schema_obj)
    res = jsonschema.Draft201909Validator(schema_obj)
    print(res)
    print('Schema validates against https://json-schema.org/draft/2019-09/schema')

    #metaschema = json.load(open('schemas/caspian_metaschema.json'))
    #schema = json.load(open('schemas/yellow_taxi.schema.json'))
    #print('metaschema ', metaschema['title'])
    #print('schema ', schema)
    #jsonschema.validate(schema_obj, metaschema)
#print(schema_obj)
#schema_obj["$schema"] = "http://json-schema.org/draft-07/schema#"
#schema = json.dumps(schema_obj, indent=2)
#test = json.loads(schema)
#print(schema)

#with open('./schemas/json-schema.201909.json') as fh:
#    schema201909 = json.load(fh)
#jsonschema.validate(schema_obj, schema201909)
#jsonschema.check_schema(dict(schema_obj))





# Simple schema generation test

#fieldname1 = FieldNames(rawname='tpep_pickup_datetime')
#fieldtype1 = FieldTypes(ftype='int64')
#datafield1 = DataField(names=fieldname1, types=fieldtype1)

#fieldname2 = FieldNames(rawname='tpep_pickup_datetime')
#fieldtype2 = FieldTypes(ftype='double')
#datafield2 = DataField(names=fieldname2, types=fieldtype2)

#tabular = Tabular(fields=[datafield1, datafield2])
#model = CaspianSchema(model=tabular)

#print('printing schema')
#print(model.schema_json(indent=2))  # This prints the whole meta-schema

#print(model.dict())  # This prints a dict of just the definied schema

#print(model.json(indent=2))  # This prints just the defined model schema as JSON






