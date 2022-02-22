from typing import List, Optional, Union, Any, Callable
from pydantic import BaseModel

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

class Transforms(BaseModel):
    # Transform definitions
    StrToDate: Optional[Callable]
    MilesToKm: Optional[Callable]
    LatLonToH3: Optional[Callable]

class FieldTransforms(BaseModel):
    # Transforms section of DataField - a list of Transforms
    transforms: Optional[List[Transforms]]

class FieldConstraints(BaseModel):
    # Constraints section of DataField
    equal: int
    notEqual: int
    greater: int
    greaterEqual: int
    less: int
    lessEqual: int
    multipleOf: int
    monotonicAsc: int
    monotonicDesc: int
    matches: str

class DataField(BaseModel):
    names: FieldNames
    types: FieldTypes
    flags: Optional[FieldFlags]
    #transforms: Optional[FieldTransforms]
    constraints: Optional[FieldConstraints]

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
    # dataset: Optional[DataSetData]
    model: Union[Tabular, Relational, Graph, Nested]


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
    with open(fnam, 'w') as f:
        f.write(CaspianSchema.schema_json(indent=2))

#write_schema('schemas/caspian_metaschema.json')



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






