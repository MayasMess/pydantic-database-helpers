from pydantic import BaseModel, Field
from datetime import datetime, date
from typing import Optional, ClassVar
from decimal import Decimal


class SimpleTable(BaseModel):
    __TABLE_NAME__: ClassVar[str] = "simple_table"

    id: int
    name: Optional[str] = Field(max_length=100)
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    is_active: Optional[bool]
    salary: Optional[float]
    birth_date: Optional[date]
    decimal_value: Optional[Decimal]


class NoTableNameModel(BaseModel):
    id: int
    name: str
