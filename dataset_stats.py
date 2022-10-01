from pydantic import BaseModel, Field
from typing import Dict, Any


class DataSetField(BaseModel):
    # TODO: set exact type where applicable
    field_name: Any = Field(default=None)
    dtype: Any = Field(default=None)
    min: Any = Field(default=None)
    max: Any = Field(default=None)
    nulls: Any = Field(default=None)
    logical: Any = Field(default=None)
    arrowtype: Any = Field(default=None)
    value_counts: Any = Field(default=None)
    distinct: Any = Field(default=None)


class DataSetInfo(BaseModel):
    num_columns: int = Field(default=0)
    num_rows: int = Field(default=0)
    num_row_groups: int = Field(default=0)
    fields: Dict[str, DataSetField] = Field(default={})
