from __future__ import annotations

from typing import List, Union, Dict, Any
from uuid import UUID
from pydantic import BaseModel, Field

class VarSelector(BaseModel):
    SID: Union[UUID, str, int] | None = None
    VID: UUID
    Selector: str = Field(default="latest") # latest=buffer | or measurement ID

    class Config:
        validate_by_name = True
        frozen = True


class BufferRequest(BaseModel):
    Start: float = -20_000
    End: float = 0
    Variables: List[VarSelector]
    Points: int = 655
    Type: str = "equidistant"
    Format: str = "json"
    Precision: int = -1
    TimeZone: str = "UTC" # Europe/Vienna
    TimeOffset: int = 0

    class Config:
        validate_by_name = True
        frozen = True


class HistoryRequest(BaseModel):
    SID: Union[UUID, str, int]
    MID: Union[UUID, str, int]
    Start: float = 0
    End: float = 0
    Variables: List[UUID]
    Points: int = 2048
    Type: str = "equidistant"
    Format: str = "json"
    Precision: int = -1
    TimeZone: str = "UTC"
    TimeOffset: int = 0

    class Config:
        validate_by_name = True
        frozen = True

class TimeSeries(BaseModel):
    Type: str
    Format: str
    Unit: str
    Start: float
    AbsoluteStart: float
    Delta: float
    End: float
    Size: int
    MeasurementId: Union[UUID, str, int]
    Updating: bool | None = None
    Values: List[List[float | None]]

    class Config:
        validate_by_name = True
        frozen = True


class _Item(BaseModel):
    TimeSeries: TimeSeries


class BufferSuccess(BaseModel):
    Success: bool
    Data: Union[_Item, List[_Item]]

    def first_timeseries(self) -> TimeSeries:
        first = self.Data[0] if isinstance(self.Data, list) else self.Data
        return first.TimeSeries


class HistorySuccess(BufferSuccess):
    pass  # identical layout


class GIStream(BaseModel):
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
    id: str = Field(alias="Id")
    name: str = Field(alias="Name")
    gql_id: str | None = Field(alias="GQLId", default=None)
    index: int = Field(alias="Index")
    unit: str = Field(alias="Unit")
    data_type: str = Field(alias="DataFormat")
    sid: Union[str, int, UUID]

    class Config:
        validate_by_name = True
        frozen = True

class GIOnlineVariable(BaseModel):
    id: UUID = Field(alias="Id")
    name: str = Field(alias="Name")
    data_type: str = Field(alias="DataFormat")
    unit: str = Field(alias="Unit")
    direction: str = Field(alias="Direction")  # "I" or "O"
    index: int = Field(alias="Index")

    index_in: int = Field(alias="IndexIn")
    index_out: int = Field(alias="IndexOut")
    precision: int = Field(alias="Precision")
    range_min: float | None = Field(alias="RangeMin")
    range_max: float | None = Field(alias="RangeMax")

    class Config:
        validate_by_name = True
        frozen = True
        extra = "ignore"


class GIHistoryMeasurement(BaseModel):
    id: Union[UUID, int] = Field(alias="Id")
    name: str = Field(alias="Name")
    absolute_start: float = Field(alias="AbsoluteStart")
    last_ts: float = Field(alias="LastTimeStamp")

    # keep any extra keys we don’t care about
    model_config = dict(validate_by_name=True, frozen=True, extra="ignore")
