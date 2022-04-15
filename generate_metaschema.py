from typing import List, Dict, Optional, Union, Any, Callable
from pydantic import BaseModel
# import json
import jsonschema

# Generate Caspian Meta-Schema

class FieldNames(BaseModel):
    # Names section of DataField
    uid: str
    raw_name: str
    display_name: Optional[str]
    description_short: Optional[str]
    description_long: Optional[str]

class FieldTypes(BaseModel):
    # Types section of DataField
    pq_type: str
    logical_type: Optional[str]
    arrow_type: Optional[str]
    representation: Optional[str]
    units: Optional[str]

class FieldFlags(BaseModel):
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

class FieldTransforms(BaseModel):
    # Transforms section of DataField - a list of Transforms
    transforms: Optional[List[Transforms]]

class Constraint(BaseModel):
    # Constraints section of DataField
    name: str
    values: Dict[str, Any]
    enabled: bool

class Status(BaseModel):
    status: str

class DataField(BaseModel):
    status: Optional[Status]
    names: FieldNames
    types: FieldTypes
    flags: Optional[FieldFlags]
    #transforms: Optional[FieldTransforms]
    constraints: Optional[List[Constraint]]

class TableData(BaseModel):
    # Table metadata
    name: str
    uid: str

class DataSetData(BaseModel):
    name: str
    datatype: str
    uid: str

class Tabular(BaseModel):
    status: Optional[Status]
    table_data: Optional[TableData]
    fields: Optional[List[DataField]]
    table_constraints: Optional[List[Constraint]]

class Relational(BaseModel):
    relational: List[Tabular]

class Graph(BaseModel):
    name: str

class Nested(BaseModel):
    name: str

class CaspianSchema(BaseModel):
    """
    This is the Caspian meta-schema
    """
    class Config:
        schema_extra = {
            '$schema': 'https://json-schema.org/draft/2019-09/schema'
        }
    dataset: DataSetData
    model: Union[Tabular, Relational, Graph, Nested]


def field_dict():
    # Utility function to return a dict of sub-fields of a DataField
    fdict = {}
    subfields = []
    dfields = DataField.__fields__
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
    jsonout = CaspianSchema.schema_json(indent=2)
    print('schema = ', jsonout)
    with open(fnam, 'w') as f:
        f.write(jsonout)

if __name__ == '__main__':
    write_metaschema('schemas/caspian_metaschema.json')
    schema_obj = CaspianSchema.schema()
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











