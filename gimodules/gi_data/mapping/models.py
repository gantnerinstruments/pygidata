from __future__ import annotations

from typing import List, Literal, Union, Dict, Any
from uuid import UUID

from pydantic import BaseModel, Field


class VarSelector(BaseModel):
    """SID–VID selector used in `/buffer/data` requests."""

    SID: Union[UUID, str, int]
    VID: UUID
    Selector: str = Field(default="latest")

    class Config:
        validate_by_name = True
        frozen = True


class BufferRequest(BaseModel):
    """Validated body for `/buffer/data`."""

    Start: float = -20_000
    End: float = 0
    Variables: List[VarSelector]
    Points: int = 655
    Type: str = "equidistant"
    Format: str = "json"
    Precision: int = -1
    TimeZone: str = "Europe/Vienna"
    TimeOffset: int = 0

    class Config:
        validate_by_name = True
        frozen = True


class TimeSeries(BaseModel):
    """Time‑series payload inside the Success→Data list."""

    Type: str
    Format: str
    Unit: str
    Start: float
    AbsoluteStart: float
    Delta: float
    End: float
    Size: int
    MeasurementId: Union[UUID, str, int]
    Updating: bool
    Values: List[List[float]]

    class Config:
        validate_by_name = True
        frozen = True


class BufferItem(BaseModel):
    TimeSeries: TimeSeries

class BufferSuccess(BaseModel):
    Success: bool
    Data: List[BufferItem]

    def first_timeseries(self) -> TimeSeries:
        return self.Data[0].TimeSeries

    def timeseries_list(self) -> List[TimeSeries]:
        return [item.TimeSeries for item in self.Data]



class GIStream(BaseModel):
    """Descriptor of a buffer or history stream."""

    name: str = Field(alias="Name")
    id: Union[UUID, int] = Field(alias="Id")
    sample_rate_hz: float = Field(alias="SampleRateHz")
    first_ts: float = Field(alias="AbsoluteStart")
    last_ts: float = Field(alias="LastTimeStamp")
    index: int = Field(alias="Index")

    class Config:
        validate_by_name = True
        frozen = True


class GIStreamVariable(BaseModel):
    """Metadata for one variable inside a stream."""

    id: str = Field(alias="Id")
    name: str = Field(alias="Name")
    index: int = Field(alias="Index")
    unit: str = Field(alias="Unit")
    data_type: str = Field(alias="DataFormat")
    sid: Union[str, int, UUID]

    class Config:
        validate_by_name = True
        frozen = True