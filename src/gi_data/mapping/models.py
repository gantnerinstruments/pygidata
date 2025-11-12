from __future__ import annotations

from typing import List, Union, Optional, TYPE_CHECKING
from uuid import UUID

from pydantic import BaseModel, Field, PrivateAttr

if TYPE_CHECKING:
    from gi_data import GIDataClient


class VarSelector(BaseModel):
    SID: Union[UUID, str, int] | None = None
    VID: UUID | str
    Selector: str = Field(default="latest")  # latest=buffer | or measurement ID

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
    TimeZone: str = "UTC"  # Europe/Vienna
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
    id: Union[str, UUID, int] = Field(alias="Id")
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


class HistoryRequest(BaseModel):
    Start: float = 0
    End: float = 0
    Variables: List[VarSelector]
    Points: int = 2048
    Type: str = "equidistant"
    Format: str = "json"
    Precision: int = -1
    TimeZone: str = "UTC"
    TimeOffset: int = 0
    AddVarMapping: bool = True

    class Config:
        validate_by_name = True
        frozen = True


class GIHistoryVariable(BaseModel):
    id: Union[UUID, str] = Field(alias="Id")
    name: str = Field(alias="Name")
    data_format: str = Field(alias="DataFormat")
    direction: str = Field(alias="Direction")
    type: str = Field(alias="Type")
    index: int = Field(alias="Index")
    index_in: int = Field(alias="IndexIn")
    index_out: int = Field(alias="IndexOut")
    precision: int = Field(alias="Precision")
    range_max: float = Field(alias="RangeMax")
    range_min: float = Field(alias="RangeMin")
    unit: str = Field(alias="Unit")
    internal_id: Optional[str] = Field(alias="_id", default=None)

    model_config = dict(validate_by_name=True, frozen=True, extra="ignore")


class GIHistoryMeasurement(BaseModel):
    id: Union[UUID, int, str] = Field(alias="Id")
    absolute_start: float = Field(alias="AbsoluteStart")
    last_ts: float = Field(alias="LastTimeStamp")
    sample_rate_hz: float = Field(alias="SampleRateHz")
    source_id: Union[UUID, int, str] = Field(alias="SourceId")

    # Optional (local-only metadata)
    name: Optional[str] = Field(alias="Name", default=None)
    available_time_sec: Optional[float] = Field(alias="AvailableTimeSec", default=None)
    cfg_checksum: Optional[str] = Field(alias="CfgCheckSum", default=None)
    data_storage: Optional[str] = Field(alias="DataStorage", default=None)
    index: Optional[int] = Field(alias="Index", default=None)
    is_removable: Optional[bool] = Field(alias="IsRemovable", default=None)
    kind: Optional[str] = Field(alias="Kind", default=None)
    max_time_sec: Optional[float] = Field(alias="MaxTimeSec", default=None)
    start_date: Optional[str] = Field(alias="StartDate", default=None)
    updated: Optional[bool] = Field(alias="Updated", default=None)

    # Present on local only; cloud = None until resolved
    variables: Optional[List[GIHistoryVariable]] = Field(alias="Variables", default=None)

    internal_id: Optional[str] = Field(alias="_id", default=None)

    # Internal reference to client, set by DataClient.list_history_measurements
    _client: Optional["GIDataClient"] = PrivateAttr(default=None)

    model_config = dict(
        validate_by_name=True,
        frozen=False,
        extra="ignore",
    )

    def attach_client(self, client: "GIDataClient") -> "GIHistoryMeasurement":
        self._client = client
        return self

    @property
    def vars(self) -> List[GIHistoryVariable]:
        # Local case
        if self.variables is not None:
            return self.variables

        # Cloud case -> fetch need to load variables since gql doesnt include them
        if not self._client:
            raise RuntimeError(
                "No client attached to measurement. "
                "Call .attach_client(client) in DataClient.list_history_measurements."
            )

        vars = self._client.list_buffer_variables(self.source_id)
        # cache for next access
        self.variables = vars
        return vars


class CSVSettings(BaseModel):
    HeaderText: Optional[str] = None
    AddColumnHeader: bool = True
    DateTimeHeader: str = "datetime"
    DateTimeFormat: str = "%Y-%m-%d %H:%M:%S.%%0us"
    ColumnSeparator: str = ";"
    DecimalSeparator: str = "."
    NameRowIndex: Optional[int] = None
    UnitRowIndex: Optional[int] = None
    ValuesStartRowIndex: Optional[int] = None
    ValuesStartColumnIndex: Optional[int] = None

    model_config = dict(validate_by_name=True, frozen=True)


class CSVImportSettings(BaseModel):
    ColumnSeparator: str = ";"
    DecimalSeparator: str = ","
    NameRowIndex: int = 0
    UnitRowIndex: int = -1
    ValuesStartRowIndex: int = 1
    ValuesStartColumnIndex: int = 1

    # Backend uses "%d.%m.%Y %H:%M:%S.%F" -> Python uses "%f"
    # Provide override if needed.
    DateTimeFmtColumn1: str = "%Y-%m-%d %H:%M:%S.%F"
    DateTimeFmtColumn2: str = ""
    DateTimeFmtColumn3: str = ""


class CSVImportSettingsDefaultCloud(CSVImportSettings):
    NameRowIndex: int = 0
    UnitRowIndex: int = 3
    ValuesStartRowIndex: int = 4
    ValuesStartColumnIndex: int = 2

    DateTimeFmtColumn1: str = "%Y-%m-%d %H:%M:%S.%F"


class LogSettings(BaseModel):
    SourceID: str
    SourceName: str
    MeasurementName: Optional[str] = None

    model_config = dict(validate_by_name=True, frozen=True)
