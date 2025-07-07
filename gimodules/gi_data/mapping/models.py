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

    Start: int = -20_000
    End: int = 0
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


class BufferSuccess(BaseModel):
    """Top-level Basic Success wrapper."""

    Success: bool
    Data: Dict

    def first_timeseries(self) -> TimeSeries:
        return TimeSeries.model_validate(self.Data["TimeSeries"])


class GIStream(BaseModel):
    """Descriptor of a buffer or history stream."""

    name: str = Field(alias="Name")
    id: Union[UUID, int] = Field(alias="Id")
    sample_rate_hz: float = Field(alias="SampleRateHz")
    first_ts: int = Field(alias="AbsoluteStart")
    last_ts: int = Field(alias="LastTimeStamp")
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
    sid: Union[str, int]

    class Config:
        validate_by_name = True
        frozen = True