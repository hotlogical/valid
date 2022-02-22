from typing import List, Optional, Union, Any, Callable
from pydantic import BaseModel


class FieldNames(BaseModel):
    # Names
    rawname: str
    displayname: Optional[str]
    descriptionshort: Optional[str]
    descriptionlong: Optional[str]


class FieldTypes(BaseModel):
    # Type info
    ftype: str
    logicaltype: Optional[str]
    arrowtype: Optional[str]
    representation: Optional[str]
    units: Optional[str]


class FieldFlags(BaseModel):
    # Flags
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
    StrToDate: Optional[Callable]
    MilesToKm: Optional[Callable]
    LatLonToH3: Optional[Callable]


class FieldTransforms(BaseModel):
    transforms: Optional[List[Transforms]]


class FieldConstraints(BaseModel):
    # Constraints
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


class Tabular(BaseModel):
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
    model: Union[Tabular, Relational, Graph, Nested]
#    model: Tabular

# with open('schemas/caspian_metaschema.json', 'w') as f:
def write_schema(fnam):
    with open(fnam, 'w') as f:
        f.write(CaspianSchema.schema_json(indent=2))

#write_schema('schemas/caspian_metaschema.json')


#model = CaspianSchema(model={'fields': [
#    {'names': {'FieldRawName': 'poo'}}
#]})


#fieldname1 = FieldNames(rawname='tpep_pickup_datetime')
#fieldtype1 = FieldTypes(ftype='int64')
#datafield1 = DataField(names=fieldname1, types=fieldtype1)

#fieldname2 = FieldNames(rawname='tpep_pickup_datetime')
#fieldtype2 = FieldTypes(ftype='double')
#datafield2 = DataField(names=fieldname2, types=fieldtype2)

#tabular = Tabular(fields=[datafield1, datafield2])
#model = CaspianSchema(model=tabular)

#print('printing schema')
#print(model.schema_json(indent=2))

#print(model.dict())

#model2 = CaspianSchema(model = {'fields': [{'names': {'fieldrawname': 'tpep_pickup_datetime'}, 'types': {'ftype': 'int64'}}, {'names': {'fieldrawname': 'tpep_dropoff_datetime'}, 'types': {'ftype': 'double'}}]})

#print(model2.dict())
#print(model.json(indent=2))



#dfields = DataField.__fields__
#print(dfields)
#for df in dfields:
#    field = dfields[df]
#    print(df)
#    typ = field.type_
#    flds = typ.__fields__
#    for fld in flds:
#        print('\t', fld)

def field_dict():
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

#fd = field_dict()
#print(fd)