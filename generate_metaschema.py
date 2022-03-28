from typing import List, Dict, Optional, Union, Any, Callable
from pydantic import BaseModel
import json, jsonschema

# Generate Caspian Meta-Schema

class FieldNames(BaseModel):
    # Names section of DataField
    rawname: str
    displayname: Optional[str]
    descriptionshort: Optional[str]
    descriptionlong: Optional[str]

class FieldTypes(BaseModel):
    # Types section of DataField
    ftype: str
    logicaltype: Optional[str]
    arrowtype: Optional[str]
    representation: Optional[str]
    units: Optional[str]

class FieldFlags(BaseModel):
    # Flags section of DataField
    isRaw: Optional[bool]
    isPII: Optional[bool]
    isUnique: Optional[bool]
    isStandardised: Optional[bool]
    isTransformed: Optional[bool]
    inInherited: Optional[bool]
    isJoin: Optional[bool]
    isKey: Optional[bool]
    isSynthetic: Optional[bool]
    isBackfilled: Optional[bool]
    isCategorical: Optional[bool]
    isNumeric: Optional[bool]

class Transforms(BaseModel):
    # Transform definitions
    StrToDate: Optional[Callable]
    MilesToKm: Optional[Callable]
    LatLonToH3: Optional[Callable]

class FieldTransforms(BaseModel):
    # Transforms section of DataField - a list of Transforms
    transforms: Optional[List[Transforms]]

class Constraint(BaseModel):
    # Constraints section of DataField
    name: str
    values: Dict[str, Any]
    enabled: bool

class DataField(BaseModel):
    names: FieldNames
    types: FieldTypes
    flags: Optional[FieldFlags]
    #transforms: Optional[FieldTransforms]
    constraints: Optional[List[Constraint]]

class TableConstraints(BaseModel):
    # Constraints on the whole table
    Sum: int

class TableData(BaseModel):
    # Table metadata
    name: str

class DataSetData(BaseModel):
    name: str

class Tabular(BaseModel):
    tabledata: Optional[TableData]
    fields: List[DataField]
    # tableconstraints: Optional[TableConstraints]

class Relational(BaseModel):
    relational: List[Tabular]

class Nested(BaseModel):
    name: Optional[str]

class Graph(BaseModel):
    name: str

class CaspianSchema(BaseModel):
    """
    This is the Caspian meta-schema
    """
    class Config:
        schema_extra = {
            '$schema': 'https://json-schema.org/draft/2019-09/schema'
        }
    # dataset: Optional[DataSetData]
    model: Union[Tabular, Relational, Graph, Nested]
    # model: Union[Graph, Nested]
    #model: str

def field_dict():
    # Utility function to return a dict of sub-fields of a DatField
    fdict = {}
    subfields = []
    dfields = DataField.__fields__
    print('dfields ', dfields)
    for fieldgroup in dfields:
        field = dfields[fieldgroup]
        print('\tfield blob', field)
        typ = field.type_
        subfields = list(typ.__fields__.keys())
        print('\tsubfields', subfields)
        fdict[fieldgroup] = subfields
    return fdict

def write_schema(fnam):
    # Write the schema as JSON
    jsonout = CaspianSchema.schema_json(indent=2)
    print('schema = ', jsonout)
    with open(fnam, 'w') as f:
        f.write(jsonout)

if __name__ == '__main__':
    write_schema('schemas/caspian_metaschema.json')
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






