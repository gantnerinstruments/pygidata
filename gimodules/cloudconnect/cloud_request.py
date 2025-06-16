"""
Module to send simplified http request to the Cloud. (Gantner HTTP API for more information)
"""

from __future__ import annotations

import datetime as dt
import logging
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from io import BytesIO
from pathlib import Path
from typing import List, Dict, Optional, Union, Any, Type, cast, Tuple

import numpy as np
import pandas as pd
import pytz
import requests
from dateutil import tz, relativedelta
from requests.auth import HTTPBasicAuth

from gimodules.cloudconnect import utils, authenticate
from gimodules.cloudconnect.exceptions import ImportTimestampError

# Set output level to INFO because default is WARNNG
logging.getLogger().setLevel(logging.INFO)


@dataclass
class CsvConfig:
    """Object for tracking parameters for csv import"""

    ColumnSeparator: str = ";"
    DecimalSeparator: str = ","
    NameRowIndex: int = 0
    UnitRowIndex: int = 0
    ValuesStartRowIndex: int = 1
    ValuesStartColumnIndex: int = 1
    # Column 1: Date and Time -> specified in Gantner http docs
    # Comment one out: if python formatter differs from C++ formatter
    # "%d.%m.%Y %H:%M:%S.%F" on backend -> "%d.%m.%Y %H:%M:%S.%f" for python
    DateTimeFmtColumn1: str = "%d.%m.%Y %H:%M:%S.%F"
    DateTimeFmtColumn2: str = ""
    DateTimeFmtColumn3: str = ""
    SourceID: str = ""
    RetentionTime: int = 0
    Target: str = "stream"
    TimeOffSetSec: int = 0
    AddTimeSeries: bool = False
    MeasID: str = ""

    def get_CSVSettings(self):
        """returns config as dict"""
        return {
            "ColumnSeparator": self.ColumnSeparator,
            "DecimalSeparator": self.DecimalSeparator,
            "NameRowIndex": self.NameRowIndex,
            "UnitRowIndex": self.UnitRowIndex,
            "ValuesStartRowIndex": self.ValuesStartRowIndex,
            "ValuesStartColumnIndex": self.ValuesStartColumnIndex,
            "DateTimeFmtColumn1": self.DateTimeFmtColumn1,
            "DateTimeFmtColumn2": self.DateTimeFmtColumn2,
            "DateTimeFmtColumn3": self.DateTimeFmtColumn3,
            "SourceID": self.SourceID,
        }


@dataclass()
class GIStream:
    """Object for tracking available streams"""

    name: str
    id: str
    sample_rate_hz: str
    first_ts: int
    last_ts: int
    index: int


@dataclass()
class GIStreamVariable:
    """Object for tracking available variables"""

    id: str
    name: str
    index: str
    unit: str
    data_type: str
    sid: str


class Helpers:
    @staticmethod
    def remove_hex_from_string(string: str) -> str:
        """Remove hex value from input string"""
        return re.sub(r"[^\x00-\x7f]", r"", string)


class Resolution(Enum):
    MONTH = "MONTH"
    WEEK = "WEEK"
    DAY = "DAY"
    HOUR = "HOUR"
    QUARTER_HOUR = "QUARTER_HOUR"
    MINUTE = "MINUTE"
    SECOND = "SECOND"
    HZ10 = "HZ10"
    HZ100 = "HZ100"
    KHZ = "KHZ"
    KHZ10 = "KHZ10"
    NANOS = "nanos"


class DataFormat(Enum):
    COL = "col"
    ROW = "row"
    JSON = "json"
    CSV = "csv"
    UDBF = "udbf"
    FAMOS = "famos"
    MDF = "mdf"
    MAT = "mat"
    WAV = "wav"
    RPC = "rpc"


class DataType(Enum):
    EQUIDISTANT = "equidistant"
    ABSOLUTE = "absolute"
    AUTO = "auto"
    FFT = "fft"


class Variable():
    SID: str
    VID: str
    Selector: str


class CSVSettings():
    HeaderText: str
    AddColumnHeader: bool
    DateTimeHeader: str
    DateTimeFormat: str
    ColumnSeparator: str
    DecimalSeparator: str


class LogSettings():
    SourceID: str
    SourceName: str
    MeasurementName: str


def get_sample_rate(resolution: str):
    if resolution == "MONTH":
        return 1 / (30 * 24 * 60 * 60)
    elif resolution == "WEEK":
        return 1 / (7 * 24 * 60 * 60)
    elif resolution == "DAY":
        return 1 / (24 * 60 * 60)
    elif resolution == "HOUR":
        return 1 / (60 * 60)
    elif resolution == "QUARTER_HOUR":
        return 1 / (15 * 60)
    elif resolution == "MINUTE":
        return 1 / 60
    elif resolution == "SECOND":
        return 1
    elif resolution == "HZ10":
        return 10
    elif resolution == "HZ100":
        return 100
    elif resolution == "KHZ":
        return 1_000
    elif resolution == "KHZ10":
        return 10_000
    elif resolution == "nanos":
        return 1 / 1e9
    else:
        return 1


class CloudRequest:
    def __init__(self) -> None:
        self.stream_variables = None
        self.url: Optional[str] = ""
        self.user: str = ""
        self.pw: str = ""
        self.login_token: Optional[Dict[str, Optional[str]]] = None
        self.refresh_token: Optional[str] = None
        self.streams: Optional[Dict[str, GIStream]] = None
        self.stream_variables: Optional[Dict[str, GIStreamVariable]] = None
        self.query: str = ""
        self.request_measurement_res = None
        self.timezone: str = "Europe/Vienna"

        # Enums
        self.resolutions = Resolution
        self.units: Type[Any] = cast(Type[Enum], Enum("Units", {}))

        # Importer data
        self.import_session_res_udbf = None
        self.import_session_res_csv = None
        self.import_session_csv_current: Optional[dict] = None
        self.session_ID = None
        self.csv_config = CsvConfig()

    def login(
            self,
            url: Optional[str] = None,
            user: Optional[str] = None,
            password: Optional[str] = None,
            access_token: Optional[str] = None,
            use_env_file: bool = False,
            dotenv_path: Optional[str] = ".env",
    ) -> None:
        """Login method that handles Bearer Token/tenant,
        username/password logins, or .env file.
        .env file should look like:

        CLOUD_TENANT='https://demo.gi-cloud.io'
        BEARER_TOKEN=''
        dotenv_path example: "/path/to/custom/.env"
        """

        if url and access_token:
            self.login_token = {"access_token": access_token}
            self.url = url
        elif url and user and password:
            self.url = url
            self.user = user
            self.pw = password
        elif use_env_file:
            tenant, bearer_token, refresh_token = (
                authenticate.load_env_variables(dotenv_path=dotenv_path))
            logging.info(bearer_token)
            self.url = tenant
            self.login_token = {
                "access_token": bearer_token,
                "refresh_token": refresh_token,
            }
        else:
            raise ValueError("Invalid arguments provided for login")

        if user and password:
            login_form = {
                "username": self.user,
                "password": self.pw,
                "grant_type": "password",
            }
            auth = HTTPBasicAuth("gibench", "")
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            login_url = f"{self.url}/token"

            try:
                res = requests.post(login_url, data=login_form, headers=headers, auth=auth)
                if res.status_code == 200:
                    self.login_token = res.json()
                    self.refresh_token = (
                        (self.login_token.get("refresh_token")) if self.login_token else None
                    )
                    logging.info("Login successful")
                else:
                    logging.error(
                        f"Login failed! Response Code:" f" {res.status_code}, Reason: {res.reason}"
                    )
            except requests.RequestException as e:
                logging.warning(f"Request error during login: {e}")

        try:
            assert self.login_token, "Login token is None even after Login!!"
            # Set headers for future requests after login
            self.headers: dict = {"Authorization": f"Bearer {self.login_token['access_token']}"}
            self.get_all_stream_metadata()
            self.print_streams()
            self.get_all_var_metadata()
        except Exception as e:
            logging.error(f"Login post-processing failed: {e}")
            raise Exception(f"Login failed! {e}")

    def refresh_access_token(self) -> None:
        """
        Refresh the access token with a refresh token.
        The refresh token is valid for 14 days.
        """
        if not self.refresh_token:
            logging.error("No refresh token available for refreshing access token.")
            return

        # Prepare request
        refresh_form = {
            "ClientID": "gibench",
            "RefreshToken": self.refresh_token,
        }
        headers = {"Content-Type": "application/json"}
        refresh_url = f"{self.url}/rpc/AdminAPI.RefreshToken"

        # Send request
        try:
            res = requests.post(refresh_url, json=refresh_form, headers=headers)
            if res.status_code == 200:
                response_data = res.json()
                self.login_token = {
                    "access_token": response_data.get("AccessToken"),
                    "refresh_token": response_data.get("RefreshToken", self.refresh_token),
                }
                logging.info("Access token refreshed successfully.")
            else:
                logging.error(
                    f"Failed to refresh access token. Response Code: "
                    f"{res.status_code}, Reason: {res.reason}"
                )
        except requests.RequestException as e:
            logging.warning(f"Request error while refreshing access token: {e}")

    from typing import Optional, Dict

    def get_all_stream_metadata(self) -> Optional[Dict[Any, GIStream]]:
        """
        Loads the available meta information from all available streams.
        The data is stored in data classes (GIStream)
        and is accessible via the .streams attribute.

        Returns:
            A dictionary of stream metadata if successful, otherwise None.
        """
        if not self.login_token:
            logging.error("You have no valid access token! Please login first.")
            return None

        # Prepare request
        url_list = f"{self.url}/kafka/structure/sources"
        # Send request
        try:
            res = requests.get(url_list, headers=self.headers)
            if res.status_code == 200:
                response_data = res.json()
                self.streams = {}  # Reset memory
                for stream in response_data["Data"]:
                    name = stream["Name"]
                    stream_id = stream["Id"]
                    sample_rate_hz = stream["SampleRateHz"]
                    first_ts = stream["AbsoluteStart"]
                    last_ts = stream["LastTimeStamp"]
                    index = stream["Index"]
                    # Store data in dict with dataclass
                    self.streams[stream_id] = GIStream(
                        name,
                        stream_id,
                        sample_rate_hz,
                        first_ts,
                        last_ts,
                        index,
                    )
                return self.streams
            elif res.status_code in {401, 403}:
                self.refresh_access_token()
                res = requests.get(url_list, headers=self.headers)
                if res.status_code == 200:
                    return self.get_all_stream_metadata()
                else:
                    logging.error(f"Failed after token refresh! Code: {res.status_code}")
            else:
                logging.error(f"Failed! Code: {res.status_code}, Reason: {res.reason}")
        except requests.RequestException as e:
            logging.warning(f"Request error while fetching stream metadata: {e}")

        return None

    def get_streams_by_name(self, stream_name: str) -> Optional[List[GIStream]]:
        """
        Searches for streams by name.

        Args:
            stream_name (str): The name of the stream to search for.

        Returns:
            Optional[List[GIStream]]: A list of matching streams, or None if no matches.
        """
        if not self.streams:
            logging.info("You have no loaded streams.")
            return None

        matches = [stream for stream in self.streams.values() if stream.name == stream_name]

        if not matches:
            logging.info("No stream found.")
            return None

        return matches

    def print_streams(self) -> None:
        """
        Prints the available streams' metadata.
        """
        if not self.streams:
            logging.info("You have no loaded streams.")
            return

        for stream in self.streams.values():
            first_ts_str = dt.datetime.utcfromtimestamp(float(stream.first_ts) / 1000).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            last_ts_str = dt.datetime.utcfromtimestamp(float(stream.last_ts) / 1000).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            logging.info(
                f"{'Streamname:':<20}{stream.name}\n"
                f"{'Streamid:':<20}{stream.id}\n"
                f"{'first ts:':<20}{stream.first_ts} {first_ts_str}\n"
                f"{'last ts:':<20}{stream.last_ts} {last_ts_str}\n"
                f"{'Samplerate:':<20}{stream.sample_rate_hz}\n"
            )

    def get_all_var_metadata(self) -> None:
        """
        Loads the available meta information from all available variables
        using the REST endpoint (GQL API varmapping does not update on project config change).
        The data is stored in data classes and is accessible via the .stream_variables attribute.
        """
        if not self.streams:
            logging.info("You have no loaded streams. Please load streams first.")
            return

        url = f"{self.url}/kafka/structure/sources"
        assert self.login_token, "No valid access token. Please log in first."
        headers = {
            "Authorization": f"Bearer {self.login_token['access_token']}",
            "Content-Type": "application/json",
        }

        self.stream_variables = {}  # Reset memory
        unit_names = set()

        payload = {
            "AddVarMapping": True,
            "Sources": [stream.id for stream in self.streams.values()],
        }

        try:
            res = requests.post(url, json=payload, headers=headers)

            if res.status_code == 401 or res.status_code == 403:
                self.refresh_access_token()
                headers["Authorization"] = f"Bearer {self.login_token['access_token']}"
                res = requests.post(url, json=payload, headers=headers)

            res.raise_for_status()
            response_data = res.json()

            if not response_data.get("Success"):
                logging.error("Failed to load data: %s", response_data.get("Message"))
                return

            for source in response_data.get("Data", []):
                stream_name = source["Name"]
                sid = source["Id"]
                for variable in source.get("Variables", []):
                    try:
                        name = variable["Name"]
                        index = variable["GQLId"]
                        variable_id = variable["Id"]

                        # Use .get() for non critical in case it's missing
                        unit = variable.get("Unit", "")
                        data_type = variable.get("DataFormat", "")
                    except KeyError as missing_key:
                        if name:
                            logging.info(f"Skipping variable '{name}'"
                                         f" in stream '{stream_name}' due to "
                                         f"missing key: {missing_key}")
                        else:
                            logging.info(f"Skipping variable in stream "
                                         f"'{stream_name}' due to missing key: {missing_key}")
                        continue

                    if unit:
                        unit_names.add(unit)

                    # Create unique variable name
                    unique_var_name = f"{stream_name}__{name}"
                    self.stream_variables[unique_var_name] = GIStreamVariable(
                        variable_id, name, index, unit, data_type, sid
                    )

        except requests.RequestException as e:
            logging.error("Request error while fetching variable metadata: %s", e)
            return

        # Create Enum for available units
        self.units = cast(Type[Enum], Enum("Units", {unit: unit for unit in unit_names}))

    def variable_info(self) -> Optional[Any]:
        """
        Use this endpoint to read information for all available online variables.

        Returns:
            The parsed JSON response if successful, otherwise None.
        """
        if not self.login_token:
            logging.error("No valid access token. Please log in first.")
            return None

        # Prepare request
        url_list = f"{self.url}/online/structure/variables"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.login_token['access_token']}",
        }

        # Send request
        try:
            res = requests.get(url_list, headers=headers)
            if res.status_code == 200:
                response_data = res.json()
                # TODO: Implement processing of response_data here if needed
                return response_data
            else:
                logging.error(
                    f"Fetching variable info failed!"
                    f" Response Code: {res.status_code}, Reason: {res.reason}"
                )
        except requests.RequestException as e:
            logging.warning(f"Request error while fetching variable info: {e}")

        return None

    def find_var(self, var_name: Union[str, List[str]]) -> Optional[Dict[str, GIStreamVariable]]:
        """
        Searches the existing variables.

        Args:
            var_name (Union[str, List[str]]): Variable name or a list of substrings.

        Returns:
            Optional[Dict[str, GIStreamVariable]]: Dictionary of found variables
            or None if no matches.
        """
        if not self.stream_variables:
            logging.info("No variables are available to search.")
            return None

        # Search logic
        if isinstance(var_name, list):
            var_name_set = set(var_name)  # Use set for faster lookup
            match = [k for k in self.stream_variables.keys() if any(vr in k for vr in var_name_set)]
        else:
            match = [k for k in self.stream_variables.keys() if var_name in k]

        if not match:
            logging.info("No variable found.")
            return None

        # Create the result dictionary for matching variables
        result = {m: self.stream_variables[m] for m in match}
        return result

    def filter_var_attr(self, attr: str, value: str) -> Optional[List[GIStreamVariable]]:
        """
        Search for stream_variables with a certain attribute.

        Args:
            attr (str): The attribute to filter by (e.g., "unit").
            value (str): The value to match for the specified attribute (e.g., "C°").

        Returns:
            Optional[List[GIStreamVariable]]: A list of matching variables,
             or None if no matches found.
        """
        if not self.stream_variables:
            logging.info("No stream variables available to filter.")
            return None

        # Filtering based on the attribute and value
        match = [var for var in self.stream_variables.values() if getattr(var, attr, "") == value]

        return match if match else None

    @staticmethod
    def _build_sensorid_querystring(
            indices: List[str], aggregations: Optional[List[str]] = None
    ) -> str:
        """
        Builds a query string for the sensor IDs with optional aggregations.

        Args:
            indices (List[str]): A list of sensor indices.
            aggregations (Optional[List[str]]): A list of aggregations to apply.
            Defaults to ["avg"] if not provided.

        Returns:
            str: The constructed query string.
        """
        # Default aggregation to "avg" if none are provided
        if not aggregations:
            aggregations = ["avg"]

        # Create the aggregation string
        agg_str = " ".join(aggregations)

        # Build the query string for each index
        query_parts = [
            f"""
            {i} {{
                {agg_str}
            }}
            """
            for i in indices
        ]

        return "\n".join(query_parts)

    def get_var_data_batched(
            self,
            sid: str,
            index_list: List,
            start_date: str,
            end_date: str,
            resolution: str = "nanos",
            custom_column_names: Optional[List[str]] = None,
            timezone: str = "UTC",
            max_points: int = 700_000,
    ):
        """BETA: This makes batched calls to GraphQL. Not recommended.
        This is still about 2x slower than get_data_as_csv() since we make more http requests"""
        if custom_column_names is None:
            custom_column_names = []
        tss, tse = map(self.convert_datetime_to_unix, [start_date, end_date])
        duration_seconds = (tse - tss) // 1000
        sample_rate = get_sample_rate(resolution)
        total_points = sample_rate * len(index_list) * duration_seconds

        if total_points > max_points:
            num_batches = (total_points // max_points) + 1
            timestamps = np.linspace(tss, tse, num_batches + 1, dtype=int)
            logging.info(f"Total points: {total_points}, Num batches: {num_batches}")
            return pd.concat(
                [
                    self.get_var_data_batch(
                        sid,
                        index_list,
                        timestamps[i],
                        timestamps[i + 1],
                        resolution,
                        custom_column_names,
                        timezone,
                    )
                    for i in range(num_batches)
                ]
            ).reset_index(drop=True)
        return self.get_var_data_batch(
            sid, index_list, tss, tse, resolution, custom_column_names, timezone
        )

    def get_var_data_batch(
            self, sid, index_list, tss, tse, resolution, custom_column_names, timezone
    ):
        selected_index_string = (
            ",".join([f'"{index}"' for index in index_list]) if index_list else ""
        )
        if not selected_index_string:
            logging.info("No variable selected")
            return None

        query = f"""
                {{
                    analytics(
                        from: {tss},
                        to: {tse},
                        resolution: {resolution},
                        sid: "{sid}"
                    ) {{
                        ts
                        {self._build_sensorid_querystring(index_list)}
                    }}
                }}
            """

        res = self._execute_gql_request(query)
        if not res:
            return None

        requested_data = res.json()
        if resolution == "nanos":
            data_matrix = requested_data["data"]["Raw"]["data"]
        else:
            data_matrix = np.column_stack(
                (
                    [requested_data["data"]["analytics"]["ts"]]
                    + [requested_data["data"]["analytics"][idx]["avg"] for idx in index_list]
                )
            )

        data_matrix = np.array(data_matrix, dtype=float)
        valid_data = data_matrix[~np.isnan(data_matrix).any(axis=1)]

        columns = (
            custom_column_names if custom_column_names else self.__get_column_names(sid, index_list)
        )
        df = pd.DataFrame(valid_data, columns=columns)
        df["Time"] = pd.to_datetime(df["Time"], unit="ms")
        self.__convert_df_time_from_utc_to_tz(df, timezone)
        return df

    def _execute_gql_request(self, query):
        url = f"{self.url}/__api__/gql"
        headers = {"Authorization": f"Bearer {self.login_token['access_token']}"}
        res = requests.post(url, json={"query": query}, headers=headers)

        if res.status_code == 200 and "errors" not in res.text:
            return res
        elif res.status_code in [401, 403]:
            logging.info("Token expired. Renewing...")
            self.refresh_access_token()
            return self._execute_gql_request(query)
        logging.error(f"Request failed with code {res.status_code}: {res.text}")
        return None

    def get_var_data(
            self,
            sid: str,
            index_list: List[str],
            start_date: str,
            end_date: str,
            resolution: str = "nanos",
            custom_column_names: Optional[List[str]] = None,
            timezone: str = "UTC",
    ) -> Optional[pd.DataFrame]:
        """
        Returns a pandas DataFrame with timestamps and values directly from a data stream.

        Args:
            sid (str): Stream ID.
            index_list (List[str]): List of channel indices (e.g., ["a10", "a11"]).
            start_date (str): Start date in format "YYYY-MM-DD HH:MM:SS".
            end_date (str): End date in format "YYYY-MM-DD HH:MM:SS".
            resolution (str, optional): Data resolution. Defaults to "nanos".
            custom_column_names (Optional[List[str]], optional):
            Custom column names for the DataFrame.
            timezone (str, optional): Timezone for the data. Defaults to "UTC".

        Returns:
            Optional[pd.DataFrame]: DataFrame containing the requested data,
            or None if the request failed.
        """
        tss = str(self.convert_datetime_to_unix(start_date))
        tse = str(self.convert_datetime_to_unix(end_date))

        # Build the query
        if resolution == "nanos" and index_list:
            selected_index_string = ", ".join(f'"{idx}"' for idx in index_list)
            self.query = f"""
            {{
                Raw(columns: ["ts", "nanos", {selected_index_string}],
                sid: "{sid}",
                from: {tss},
                to: {tse}) {{
                    data
                }}
            }}
            """
        elif index_list:
            selected_index_string = self._build_sensorid_querystring(index_list)
            self.query = f"""
            {{
                analytics(
                    from: {tss},
                    to: {tse},
                    resolution: {resolution},
                    sid: "{sid}"
                ) {{
                    ts
                    {selected_index_string}
                }}
            }}
            """
        else:
            logging.info("No variable selected")
            return None

        # Send the request
        url_list = f"{self.url}/__api__/gql"
        try:
            res = requests.post(url_list, json={"query": self.query}, headers=self.headers)
            if res.status_code == 200 and "errors" not in res.text:
                requested_data = res.json()

                # Filter data out of the request
                if resolution == "nanos":
                    data_matrix = requested_data["data"]["Raw"]["data"]
                    self.data = np.array(data_matrix, dtype=float)
                else:
                    self.data = np.zeros(
                        (
                            len(requested_data["data"]["analytics"]["ts"]),
                            len(index_list) + 1,
                        )
                    )
                    self.data[:, 0] = requested_data["data"]["analytics"]["ts"]
                    for k, idx in enumerate(index_list):
                        self.data[:, k + 1] = requested_data["data"]["analytics"][idx]["avg"]

                # Clean the data by removing leading/trailing NaNs
                valid_rows = ~np.isnan(self.data).any(axis=1)
                self.data = self.data[valid_rows]

                # Create the DataFrame
                if resolution == "nanos":
                    column_names = ["Nanos"] + (custom_column_names
                                                or self.__get_column_names(sid, index_list))
                    self.df = pd.DataFrame(self.data, columns=column_names)
                    self.df["Time"] = pd.to_datetime(self.df["Nanos"], unit="ms")

                else:
                    column_names = custom_column_names or self.__get_column_names(sid, index_list)
                    self.df = pd.DataFrame(self.data, columns=column_names)
                    self.df["Time"] = pd.to_datetime(self.df["Time"], unit="ms")

                # Convert time column to datetime and adjust timezone
                self.__convert_df_time_from_utc_to_tz(self.df, timezone)

                return self.df
            elif res.status_code in {401, 403}:
                logging.info("Token expired. Renewing...")
                self.refresh_access_token()
                # Retry once after refreshing the token
                return self.get_var_data(
                    sid,
                    index_list,
                    start_date,
                    end_date,
                    resolution,
                    custom_column_names,
                    timezone,
                )
            else:
                error = res.json().get("errors", [{}])[0].get("message", "Unknown error")
                logging.error(
                    f"Fetching data failed!"
                    f"Response Code: {res.status_code}, Reason: {res.reason}, Msg: {error}"
                )
        except requests.RequestException as e:
            logging.warning(f"Request error while fetching variable data: {e}")

        return None

    def get_data_np(
            self,
            sid: str,
            index_list: List[str],
            tss: str,
            tse: str,
            resolution: str = "nanos",
    ) -> Optional[np.ndarray]:
        """
        Returns a numpy matrix of data with timestamps and values directly from a data stream.

        Args:
            sid (str): Stream ID.
            index_list (List[str]): List of channel indices (e.g., ["a10", "a11"]).
            tss (str): Start timestamp.
            tse (str): End timestamp.
            resolution (str, optional): Data resolution. Defaults to "nanos".

        Returns:
            Optional[np.ndarray]: Numpy matrix containing the requested data,
             or None if the request failed.
        """
        if not index_list:
            logging.info("No variable selected.")
            return None

        # Build the query
        if resolution == "nanos":
            selected_index_string = ", ".join(f'"{idx}"' for idx in index_list)
            self.query = f"""
            {{
                Raw(columns: ["ts", "nanos", {selected_index_string}],
                sid: "{sid}",
                from: {tss},
                to: {tse}) {{
                    data
                }}
            }}
            """
        else:
            selected_index_string = self._build_sensorid_querystring(index_list)
            self.query = f"""
            {{
                analytics(
                    from: {tss},
                    to: {tse},
                    resolution: {resolution},
                    sid: "{sid}"
                ) {{
                    ts
                    {selected_index_string}
                }}
            }}
            """

        # Send the request
        url_list = f"{self.url}/__api__/gql"
        try:
            res = requests.post(url_list, json={"query": self.query}, headers=self.headers)
            if res.status_code == 200 and "errors" not in res.text:
                requested_data = res.json()

                # Filter data out of the request
                if resolution == "nanos":
                    data_matrix = requested_data["data"]["Raw"]["data"]
                    self.data = np.array(data_matrix, dtype=float)
                else:
                    ts_data = requested_data["data"]["analytics"]["ts"]
                    self.data = np.zeros((len(ts_data), len(index_list) + 1), dtype=float)
                    self.data[:, 0] = ts_data
                    for k, idx in enumerate(index_list):
                        self.data[:, k + 1] = requested_data["data"]["analytics"][idx]["avg"]

                # Clean the data by removing leading/trailing NaNs
                valid_rows = ~np.isnan(self.data).any(axis=1)
                self.data = self.data[valid_rows]

                return self.data
            elif res.status_code in {401, 403}:
                logging.info("Token expired. Renewing...")
                self.refresh_access_token()
                # Retry once after refreshing the token
                return self.get_data_np(sid, index_list, tss, tse, resolution)
            else:
                error = res.json().get("errors", [{}])[0].get("message", "Unknown error")
                logging.error(
                    f"Fetching data failed!"
                    f" Response Code: {res.status_code}, Reason: {res.reason}, Msg: {error}"
                )
        except requests.RequestException as e:
            logging.warning(f"Request error while fetching data: {e}")

        return None

    def _get_stream_name_for_sid_vid(self, sid: str, vid: str) -> Optional[str]:
        """
        Retrieves the stream name for a given stream ID (sid) and variable ID (vid).

        Args:
            sid (str): Stream ID.
            vid (str): Variable ID.

        Returns:
            Optional[str]: The stream name if found, otherwise None.
        """
        if self.stream_variables is not None:
            stream = [k for k, v in self.stream_variables.items() if v.sid == sid and v.id == vid]
            if len(stream) == 1:
                return stream[0].split("__")[0]

        logging.info("No stream variables available or matching stream not found.")
        return None

    def _get_stream_name_for_sid(self, sid: str) -> Optional[str]:
        """
        Retrieves the stream name for a given stream ID (sid).

        Args:
            sid (str): Stream ID.

        Returns:
            Optional[str]: The stream name if found, otherwise None.
        """
        if self.stream_variables is not None and self.streams is not None:
            stream = [
                gi_stream.name
                for gi_stream in self.streams.values()
                if gi_stream.id == sid
            ]
            if len(stream) == 1:
                return stream[0]

        logging.info("No stream variables available or matching stream not found.")
        return None

    def get_all_vars_of_stream(self, sid: str) -> List[GIStreamVariable]:
        """
        Retrieves all variables associated with a given stream ID (sid).

        Args:
            sid (str): Stream ID.

        Returns:
            List[GIStreamVariable]: A list of variables for the given stream ID.
        """
        if self.stream_variables is not None:
            return [v for v in self.stream_variables.values() if v.sid == sid]

        logging.info("No stream variables available.")
        return []

    def get_data_as_csv(
            self,
            variables: List[GIStreamVariable],
            resolution: str,
            start: str,
            end: str,
            filepath: str = "",
            streaming: bool = True,
            return_df: bool = True,
            write_file: bool = True,
            decimal_sep: str = ".",
            delimiter: str = ";",
            timezone: str = "UTC",
            aggregation: str = "avg",
            batch: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Returns a CSV file with the data of a given list of variables.

        Args:
            variables (List[GIStreamVariable]): List of variables to include in the export.
            resolution (str): Data resolution.
            start (str): Start date in "YYYY-MM-DD HH:MM:SS" format.
            end (str): End date in "YYYY-MM-DD HH:MM:SS" format.
            filepath (str, optional): Path to save the file. Defaults to "".
            streaming (bool, optional): Whether to stream the response. Defaults to True.
            return_df (bool, optional): Whether to return a DataFrame. Defaults to True.
            write_file (bool, optional): Whether to write the file to disk. Defaults to True.
            decimal_sep (str, optional): Decimal separator for the CSV. Defaults to ".".
            delimiter (str, optional): Field delimiter for the CSV. Defaults to ";".
            timezone (str, optional): Timezone for the export. Defaults to "UTC".
            aggregation (str, optional): Aggregation type. Defaults to "avg".
            batch (str, optional): Batch size for the export e.g "monthly", "yearly".
             Defaults to None.

        Returns:
            Optional[pd.DataFrame]: The data as a pandas DataFrame if return_df is True,
            otherwise None.
        """
        # Handle batch processing
        if batch is not None:
            if batch not in ['monthly', 'yearly']:
                raise ValueError("batch must be 'monthly', 'yearly', or None")

            intervals = self._generate_date_intervals(start, end, batch)
            all_dfs = []

            for i, (batch_start, batch_end) in enumerate(intervals):
                logging.info(f"Fetching batch: {batch_start}-{batch_end}")
                batch_df = self.get_data_as_csv(
                    variables=variables,
                    resolution=resolution,
                    start=batch_start,
                    end=batch_end,
                    filepath=filepath,
                    streaming=streaming,
                    return_df=True,
                    write_file=False,  # Prevent writing individual batch files
                    decimal_sep=decimal_sep,
                    delimiter=delimiter,
                    timezone=timezone,
                    aggregation=aggregation,
                    batch=None,  # Prevent recursion
                )
                if batch_df is not None:
                    if i == 0:
                        all_dfs.append(batch_df)
                    else:
                        # Remove first four metadata rows from subsequent batches
                        all_dfs.append(batch_df.iloc[3:])

            if not all_dfs:
                return None

            combined_df = pd.concat(all_dfs, ignore_index=True)

            if write_file:
                streams = set()
                for var in variables:
                    stream = self._get_stream_name_for_sid_vid(var.sid, var.id)
                    streams.add(stream)
                filename = (f"{'_'.join(filter(None, streams))}_"
                            f"{start}_{end}_{resolution}_{aggregation}.csv")
                filename = utils.sanitize_filename(filename)
                full_path = f"{filepath}{filename}"
                combined_df.to_csv(
                    full_path,
                    sep=delimiter,
                    decimal=decimal_sep,
                    index=False,
                )

            return combined_df if return_df else None
        # Build query and filename
        substring = ""
        streams = set()
        for var in variables:
            stream = self._get_stream_name_for_sid_vid(var.sid, var.id)
            streams.add(stream)
            substring += f"""{{
                field: "{var.sid}:{var.index}.{aggregation}",
                headers: ["{var.name}", "{stream}", "{aggregation}", "{var.unit or ''}"]
            }},"""

        filename = f"{'_'.join(filter(None, streams))}_{start}_{end}_{resolution}_{aggregation}.csv"
        filename = utils.sanitize_filename(filename)
        start_unix = str(self.convert_datetime_to_unix(start))
        end_unix = str(self.convert_datetime_to_unix(end))
        self.query = f"""
        {{
            exportCSV(
                resolution: {resolution}
                from: {start_unix},
                to: {end_unix},
                timezone: "{timezone}",
                filename: "{filename}"
                columns: [
                    {{
                        field: "ts",
                        headers: ["datetime"],
                        dateFormat: "%Y-%m-%dT%H:%M:%S"
                    }},
                    {{
                        field: "ts",
                        headers: ["time", "", "", "[s since 01.01.1970]"]
                    }},
                    {substring}
                ]
            ) {{
                file
            }}
        }}
        """
        # Send request
        url_list = f"{self.url}/__api__/gql"
        try:
            res = requests.post(
                url_list,
                json={"query": self.query},
                headers=self.headers,
                stream=streaming,
            )
            if res.status_code == 200 and "errors" not in res.text:
                if write_file:
                    with open(f"{filepath}{filename}", "wb") as csv_file:
                        if not streaming:
                            csv_file.write(res.content)
                        else:
                            for chunk in res.iter_content(chunk_size=1024):
                                csv_file.write(chunk)
                                csv_file.flush()
                        if not return_df:
                            return None

                # Return as DataFrame
                if return_df:
                    if not streaming or not write_file:
                        return pd.read_csv(
                            BytesIO(res.content),
                            delimiter=delimiter,
                            decimal=decimal_sep,
                        )
                    else:
                        return pd.read_csv(
                            f"{filepath}{filename}",
                            delimiter=delimiter,
                            decimal=decimal_sep,
                        )
            else:
                error_message = res.json().get("errors", [{}])[0].get("message", "Unknown error")
                logging.error(
                    f"Fetching CSV data failed!"
                    f" Response Code: {res.status_code}, Reason: {res.reason}, Msg: {error_message}"
                )
        except requests.RequestException as e:
            logging.warning(f"Request error while fetching CSV data: {e}")

        return None

    def _generate_date_intervals(self, start_str: str, end_str: str, batch: str) -> \
            List[Tuple[str, str]]:
        """Generates monthly or yearly intervals between start and end dates."""
        start_dt = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")
        intervals = []
        current_start = start_dt

        while current_start < end_dt:
            if batch == 'monthly':
                next_start = (current_start.replace(day=1) + relativedelta.relativedelta(months=1))
            elif batch == 'yearly':
                next_start = (current_start.replace(month=1, day=1) + relativedelta.relativedelta(
                    years=1))
            else:
                raise ValueError("Invalid batch parameter")

            current_end = min(next_start, end_dt)  # Do NOT subtract seconds here!

            current_start_str = current_start.strftime("%Y-%m-%d %H:%M:%S")
            current_end_str = current_end.strftime("%Y-%m-%d %H:%M:%S")
            intervals.append((current_start_str, current_end_str))

            current_start = next_start

        return intervals

    def __get_column_names(self, sid: str, index_list: List[str]) -> List[str]:
        """
        Private helper method to get the column names for the DataFrame.

        Args:
            sid (str): Stream ID.
            index_list (List[str]): List of variable indices (e.g., ["a10"]).

        Returns:
            List[str]: List of column names for the DataFrame.
        """
        col_names = ["Time"]
        res = self.filter_var_attr("sid", sid)
        if res is not None:
            for i in index_list:
                for x in res:
                    if x.index == i:
                        col_names.append(x.name)
        return col_names

    @staticmethod
    def convert_datetime_to_unix(datetime_str: str) -> Optional[float]:
        """
        Converts a datetime string to a Unix timestamp in milliseconds.

        Args:
            datetime_str (str): The datetime string in "YYYY-MM-DD HH:MM:SS" format.

        Returns:
            Optional[float]: The Unix timestamp in milliseconds, or None if conversion fails.
        """
        try:
            date_time_obj = dt.datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
            timestamp_utc = date_time_obj.replace(tzinfo=tz.gettz("UTC"))
            timestamp = int(dt.datetime.timestamp(timestamp_utc)) * 1000
            return timestamp
        except ValueError as err:
            logging.error(f"Error converting '{datetime_str}' to Unix timestamp: {err}")
            return None

    def set_timezone(self, timezone: str = "Europe/Vienna") -> None:
        """
        Sets the timezone if it is valid.

        Args:
            timezone (str): The timezone to set. Defaults to "Europe/Vienna".
        """
        if self.__validate_timezone(timezone):
            self.timezone = timezone
            logging.info(f"Timezone set to: {timezone}")
        else:
            logging.warning(f"Invalid timezone: {timezone}")

    @staticmethod
    def __validate_timezone(timezone: str) -> bool:
        """
        Validates if the given timezone is recognized.

        Args:
            timezone (str): The timezone string to validate.

        Returns:
            bool: True if the timezone is valid, False otherwise.
        """
        try:
            pytz.timezone(timezone)
            return True
        except pytz.UnknownTimeZoneError:
            return False

    def __convert_df_time_from_utc_to_tz(self, df: pd.DataFrame, timezone: str = "UTC") -> None:
        """
        Converts the timestamps of the DataFrame to the desired time zone.

        Args:
            timezone (str): The target time zone. Defaults to "UTC".

        The data is initially in UTC, and the default conversion is to 'UTC'.
        """
        try:
            df["Time"] = df["Time"].dt.tz_localize("UTC")
            df["Time"] = df["Time"].dt.tz_convert(timezone)
        except (AttributeError, TypeError) as err:
            logging.error(f"Error converting DataFrame time to timezone '{timezone}': {err}")

    def get_measurement_limit(
            self,
            sid: str,
            limit: int,
            start_ts: int = 0,
            end_ts: int = 9999999999999,
            sort: str = "DESC",
    ) -> Optional[dict]:
        """
        Retrieves measurement periods for a given stream ID (sid) with a specified limit.
        Timestamps cannot be floats!

        Args:
            sid (str): Stream ID.
            limit (int): The maximum number of measurement periods to retrieve.
            start_ts (int, optional): The start timestamp. Defaults to 0.
            end_ts (int, optional): The end timestamp. Defaults to 9999999999999.
            sort (str, optional): The sort order. Defaults to 'DESC'.

        Returns:
            Optional[dict]: The response JSON if the request is successful, otherwise None.
        """
        query_measurement = f"""
        {{
            measurementPeriods(
                sid: "{sid}",
                from: {start_ts},
                to: {end_ts},
                limit: {limit},
                sort: {sort}
            ) {{
                minTs
                maxTs
                mid
                sampleRate
            }}
        }}
        """
        url_list = f"{self.url}/__api__/gql"

        try:
            res = requests.post(url_list, json={"query": query_measurement}, headers=self.headers)
            if res.status_code == 200:
                self.request_measurement_res = res.json()
                return self.request_measurement_res
            else:
                logging.error(
                    f"Fetching measurement info failed! "
                    f"Response Code: {res.status_code}, Reason: {res.reason}"
                )
        except requests.RequestException as err:
            logging.warning(f"Request error while fetching measurement info: {err}")

        return None

    def print_measurement(self) -> Optional[np.ndarray]:
        """
        Retrieves a list of measurement periods with their start and stop timestamps.

        Returns:
            Optional[np.ndarray]: A NumPy array with start and stop timestamps,
             or None if data is unavailable.
        """
        if not self.request_measurement_res or "data" not in self.request_measurement_res:
            logging.warning("Measurement data is not available.")
            return None

        periods = self.request_measurement_res["data"].get("measurementPeriods", [])
        limit = len(periods)
        measurement_list = np.zeros((limit, 2))

        for measurement_i in range(limit):
            measurement_list[measurement_i, 0] = periods[measurement_i]["minTs"]
            measurement_list[measurement_i, 1] = periods[measurement_i]["maxTs"]

        return measurement_list

    # ubdf importer
    def _create_import_session_udbf(self, sid: str, stream_name: str) -> Optional[Union[dict, str]]:
        """
        Creates an import session for a UDBF file using the HTTP API.

        Args:
            sid (str): The source ID for the data import.
            stream_name (str): The name of the data stream.

        Returns:
            Optional[Union[dict, str]]: The response JSON if the request is successful,
            otherwise None.
        """
        url_list = f"{self.url}/history/data/import"
        param = {
            "Type": "udbf",
            "SourceID": sid,
            "SourceName": stream_name,
            "MeasID": "",
            "SessionTimeoutSec": "300",
            "AddTimeSeries": "false",
            "SampleRate": "-1",
            "AutoCreateMetaData": "true",
        }
        try:
            res = requests.post(url_list, headers=self.headers, json=param)
            if res.status_code == 200:
                self.import_session_res_udbf = res.json()
                logging.info(
                    f"Import session created for UDBF file: {self.import_session_res_udbf}"
                )
                return self.import_session_res_udbf
            else:
                logging.error(
                    f"Creating import session failed! "
                    f"Response Code: {res.status_code}, Reason: {res.reason}"
                )
        except requests.RequestException as err:
            logging.error(f"Failed to create import session: {err}")

        return None

    def import_file_udbf(self, sid: str, stream_name: str, file: bytes) \
            -> Optional[requests.Response]:
        """
        Imports a UDBF file using the HTTP API.

        Args:
            file (bytes): The UDBF file content to be imported.

        Returns:
            Optional[requests.Response]: The server response if the request is successful,
             otherwise None.
        """
        self._create_import_session_udbf(sid, stream_name)
        if not self.import_session_res_udbf or "Data" not in self.import_session_res_udbf:
            logging.error("Import session not initialized. Please create an import session first.")
            return None

        self.session_ID = str(self.import_session_res_udbf["Data"]["SessionID"])
        url_list = f"{self.url}/history/data/import/{self.session_ID}"
        header_list = {
            "Content-Type": "application/octet-stream",
            "Authorization": f"Bearer {self.login_token['access_token']}",
        }
        try:
            res = requests.post(url_list, headers=header_list, data=file)
            if res.status_code == 200:
                logging.info("UDBF file successfully imported.")
                return res
            else:
                msg = f"Import failed! Response Code: {res.status_code}, Reason: {res.reason}"
                logging.error(msg)
                raise RuntimeError(msg)

        except requests.RequestException as err:
            logging.error(f"Failed to import UDBF file: {err}")
            raise

    # csv importer
    def create_import_session_csv(
            self,
            stream_ID: str,
            stream_Name: str,
            csv_config: CsvConfig,
            create_meta_data: bool = True,
            session_timeout: int = 60,
    ) -> Optional[requests.Response]:
        """
        Creates an import session for a CSV file using the HTTP API.

        Args:
            stream_ID (str): The stream ID.
            stream_Name (str): The buffer name for the new stream.
            csv_config (CsvConfig): Configuration settings for the CSV import.
            create_meta_data (bool, optional): Whether to automatically create metadata.
            Defaults to True.
            session_timeout (int, optional): Session timeout in seconds. Defaults to 60.

        Returns:
            Optional[requests.Response]: The server response if the request is successful,
            otherwise None.
        """
        url_list = f"{self.url}/history/data/import"
        param = {
            "Type": "csv",
            "SourceID": stream_ID,
            "SourceName": stream_Name,
            "SessionTimeoutSec": str(session_timeout),
            "SampleRate": "-1",
            "AutoCreateMetaData": str(create_meta_data).lower(),
            "CSVSettings": csv_config.get_CSVSettings(),
            "RetentionTimeSec": csv_config.RetentionTime,
            "Target": csv_config.Target,
            "TimeOffsetSec": csv_config.TimeOffSetSec,
            "AddTimeSeries": csv_config.AddTimeSeries,
            "MeasID": csv_config.MeasID
        }

        try:
            logging.debug(f"CSV import parameters: {param}")
            res = requests.post(url_list, headers=self.headers, json=param)
            if res.status_code == 200:
                self.import_session_res_csv = res.json()
                self.import_session_csv_current = {
                    "stream_id": stream_ID,
                    "ts": time.time(),
                    "timeout": session_timeout,
                }
                return res
            else:
                logging.error(
                    f"Creating import session failed! "
                    f"Response Code: {res.status_code}, Reason: {res.reason}"
                )
        except requests.RequestException as e:
            logging.error(f"Failed to create import session: {e}")

        return None

    def __import_file_csv(self, file: bytes) -> Optional[requests.Response]:
        """
        Imports a CSV file using the HTTP API.

        Args:
            file (bytes): The content of the CSV file to be uploaded.

        Returns:
            Optional[requests.Response]: The server response if the request is successful,
            otherwise None.
        """
        if not self.import_session_res_csv or "Data" not in self.import_session_res_csv:
            logging.error("Import session not initialized. Please create an import session first.")
            return None

        self.session_ID = str(self.import_session_res_csv["Data"]["SessionID"])
        url_list = f"{self.url}/history/data/import/{self.session_ID}"
        headers = {
            "Content-Type": "text/csv",
            "Authorization": f"Bearer {self.login_token['access_token']}",
        }

        try:
            logging.info(
                "Uploading CSV (%d bytes) to %s ... (no progress indication)", len(file), url_list
            )
            res = requests.post(url_list, headers=headers, data=file)

            if res.status_code == 200:
                logging.info("CSV file successfully imported.")
                return res
            else:  # extract error message
                error_detail = "No additional error details"
                try:
                    json_data = res.json()
                    error_detail = (json_data.get("message") or json_data.get("error")
                                    or str(json_data))
                except ValueError:
                    if res.text:
                        error_detail = res.text.strip()

                msg = (
                    f"Import failed! "
                    f"Response Code: {res.status_code}, "
                    f"Reason: {res.reason}, "
                    f"Detail: {error_detail}"
                )
                logging.error(msg)
                raise RuntimeError(msg)

        except requests.RequestException as e:
            logging.error(f"Failed to import CSV file: {e}")
            raise

    def upload_csv_file(
            self,
            stream_name: str,
            file_path: str,
            py_formatter: Optional[str] = None,
            csv_config: Optional[CsvConfig] = None,
    ) -> Optional[str]:
        """
        Import *file_path* into GI.Cloud and return the stream UUID.

        Returns ``None`` when
        - the CSV contains no newer data than already stored, or
        - the CSV header can’t be parsed, or
        - the given / generated stream ID is not a valid UUID.

        Raises ``RuntimeError`` only when the HTTP upload itself fails.
        """
        cfg = csv_config or self.csv_config

        try:
            first_ts = self._parse_first_timestamp(file_path, cfg, py_formatter)
        except Exception as exc:
            logging.error("Could not parse first timestamp in %s: %s", file_path, exc)
            return None

        try:
            stream_id = self._resolve_stream_id(stream_name, cfg)
        except ValueError as exc:  # invalid SourceID
            logging.error(exc)
            return None

        # Then use it:
        if first_ts <= self._last_import_ts(stream_id):
            raise ImportTimestampError(
                f"Skipping import for stream '{stream_id}': CSV first timestamp "
                f"({first_ts}) <= last imported timestamp ({self._last_import_ts(stream_id)})."
            )

        # open / reuse session  ➜ upload ➜ close
        self._require_csv_session(stream_id, stream_name, cfg)
        self._upload_csv_binary(file_path)
        self._delete_import_session()

        logging.info("CSV %s imported successfully into stream %s", file_path, stream_id)
        return stream_id

    @staticmethod
    def _parse_first_timestamp(
            path: str,
            cfg: CsvConfig,
            py_fmt_override: Optional[str],
    ) -> float:
        """Return the first sample’s POSIX seconds as *float*."""
        py_fmt_1 = py_fmt_override or cfg.DateTimeFmtColumn1

        rows = pd.read_csv(
            path,
            nrows=cfg.ValuesStartRowIndex,
            sep=cfg.ColumnSeparator,
            encoding="utf-8",
        )
        date_str = Helpers.remove_hex_from_string(
            rows.iat[cfg.ValuesStartRowIndex - 1, 0]
        )
        time_str = (
            rows.iat[cfg.ValuesStartRowIndex - 1, 1]
            if cfg.DateTimeFmtColumn2
            else ""
        )

        fmt = ";".join(filter(None, (py_fmt_1, cfg.DateTimeFmtColumn2)))
        raw = ";".join(filter(None, (date_str, time_str)))
        naive = dt.datetime.strptime(raw, fmt)
        aware = naive.replace(tzinfo=tz.gettz("UTC"))
        return aware.timestamp()

    def _resolve_stream_id(self, name: str, cfg: CsvConfig) -> str:
        """
        Decide which stream UUID to use (supplied, existing, or fresh).
        Raises ``ValueError`` if *SourceID* is present but not a valid UUID.
        """
        if cfg.SourceID.strip():  # 1 explicit
            sid = cfg.SourceID.strip()
            if not utils.is_valid_uuid(sid):
                raise ValueError(f"Invalid SourceID in CsvConfig: {sid}")
            return sid

        if self.streams:  # 2 reuse
            for s in self.streams.values():
                if s.name == name:
                    return s.id

        return str(uuid.uuid4())

    def _last_import_ts(self, stream_id: str) -> float:
        """Return last imported timestamp (seconds) or 0.0."""
        if self.streams and stream_id in self.streams:
            return float(self.streams[stream_id].last_ts) / 1000
        return 0.0

    def _require_csv_session(self, sid: str, name: str, cfg: CsvConfig) -> None:
        """Ensure a valid CSV import session exists for *sid*."""
        if not self.__import_session_valid(sid):
            self.create_import_session_csv(sid, name, cfg)

    def _upload_csv_binary(self, file_path: str) -> None:
        """Read *file_path* once and hand bytes to ``__import_file_csv``."""
        path = Path(file_path)
        file_size_mb = path.stat().st_size / (1024 * 1024)
        logging.info(
            "Starting CSV import: %s (%.2f MB)", path.name, file_size_mb
        )

        with path.open("rb") as fh:
            self.__import_file_csv(fh.read())

    def __import_session_valid(self, stream_id: str) -> bool:
        """
        Checks if the current import session is valid for the given stream ID.

        Args:
            stream_id (str): The stream ID to check.

        Returns:
            bool: True if the import session is valid, False otherwise.
        """
        if self.import_session_csv_current is None:
            return False

        session = self.import_session_csv_current
        session_is_valid = (
            session["stream_id"] == stream_id
            and (session["ts"] - time.time()) + 5 < session["timeout"]
        )

        return session_is_valid

    def _delete_import_session(self) -> Optional[requests.Response]:
        """
        Deletes the current import session using the HTTP API.

        Returns:
            Optional[requests.Response]: The server response if the request is successful,
             otherwise None.
        """
        if not self.session_ID:
            logging.error("No import session ID found. Cannot delete session.")
            return None

        url_list = f"{self.url}/history/data/import/{self.session_ID}"
        headers = {"Authorization": f"Bearer {self.login_token['access_token']}"}

        try:
            res = requests.delete(url_list, headers=headers)
            if res.status_code == 200:
                logging.info("Import session closed successfully.")
                return res
            else:
                logging.error(
                    f"Failed to close import session! "
                    f"Response Code: {res.status_code}, Reason: {res.reason}"
                )
        except requests.RequestException as e:
            logging.error(f"Failed to delete import session: {e}")

        return None

    # Read and Write Single Values out of live datastreams
    # Notice postprocessed data do not work with this functions !

    def read_value(self, var_ids: List[str]) -> Optional[List[Any]]:
        """
        Reads online values for a list of variable IDs.

        Args:
            var_ids (List[str]): A list of variable IDs.
            For example, var_ids=["47f32894-c6a0-11ea-81a1-02420a000368"].

        Returns:
            Optional[List[Any]]: A list of current live values if the request is successful,
            otherwise None.
        """
        url_list = f"{self.url}/online/data"
        param = {"Variables": var_ids, "Function": "read"}

        try:
            res = requests.post(url_list, headers=self.headers, json=param)
            if res.status_code == 200:
                current_live_value = res.json()
                logging.info(f"Current live value: {current_live_value}")
                return current_live_value
            else:
                logging.error(
                    f"Failed to read value! Response Code: {res.status_code}, Reason: {res.reason}"
                )
        except requests.RequestException as e:
            logging.error(f"Request error while reading value: {e}")

        return None

    def write_value_on_channel(self, var_ids: List[str], write_list: List[Any]) -> Optional[dict]:
        """
        Writes values to a list of variable IDs.

        Args:
            var_ids (List[str]): A list of variable IDs.
            For example, var_ids=["47f32894-c6a0-11ea-81a1-02420a000368"].
            write_list (List[Any]): A list of values to write. For example, write_list=["0"].

        Returns:
            Optional[dict]: The server response as a dictionary if the request is successful,
            otherwise None.
        """
        url_list = f"{self.url}/online/data"
        param = {
            "Variables": var_ids,
            "Values": write_list,
            "Function": "write",
        }

        try:
            res = requests.post(url_list, headers=self.headers, json=param)
            if res.status_code == 200:
                write_value_res = res.json()
                logging.info("Data successfully written.")
                return write_value_res
            else:
                logging.error(
                    f"Failed to write data! Response Code: {res.status_code}, Reason: {res.reason}"
                )
        except requests.RequestException as e:
            logging.error(f"Request error while writing data: {e}")

        return None

    def get_gistreamvariables(self, stream: str, variables: List[str]) -> List[GIStreamVariable]:
        """
        Retrieves GIStreamVariable instances for the given stream and variable names.

        Args:
            stream (str): The stream name.
            variables (List[str]): A list of variable names.

        Returns:
            List[GIStreamVariable]: A list of GIStreamVariable instances if found,
            otherwise None for each not found.
        """
        gi_vars = []
        for var in variables:
            result = self.find_var(f"{stream}__{var}")
            if result:
                gi_vars.append(list(result.values())[0])
        return gi_vars

    def get_buffer_data(
            self,
            start: int,
            end: int,
            variables: List[Variable],
            points: int = 1_000_000,
            data_type: DataType = DataType.EQUIDISTANT,
            data_format: DataFormat = DataFormat.JSON,
            precision: str = "-1",
            timezone: str = "UTC",
            timeoffset: int = 0,
            csv_settings: Optional[CSVSettings] = None,
            log_settings: Optional[LogSettings] = None,
            target: Optional[str] = None,
    ) -> Union[Dict, bytes]:
        """
        Fetch data from a buffer data source via API.

        Parameters:
        - start (int): Start time in ms. Use negative value if relative to 'End'.
        - end (int): End time in ms. Use 0 for 'End'.
        - variables (List[Variable]): List of variables with keys 'SID', 'VID', 'Selector'.
        - points (int): Number of data points. Default is 1_000_000.
        - data_type (DataType): Data type. Default is DataType.EQUIDISTANT.
        - data_format (DataFormat): Data format. Default is DataFormat.JSON.
        - precision (str): Precision. Default is '-1'.
        - timezone (str): Timezone specifier, e.g., 'Europe/Vienna' default is "UTC".
        - timeoffset (int): Time offset in seconds. Default is '0'.
        - csv_settings (Optional[CSVSettings]): Configuration for CSV format.
        Required if data_format is DataFormat.CSV.
        - log_settings (Optional[LogSettings]): Configuration for UDBF format.
        Required if data_format is DataFormat.UDBF.
        - target (Optional[str]): For DataFormat.UDBF format. Options: 'file', 'record'.

        Returns:
        - Union[Dict, bytes]: The response data from the API.

        Raises:
        - requests.HTTPError: If the API request fails.
        - ValueError: If required parameters are missing.
        """

        url = f"{self.url}/buffer/data"

        # Validate required parameters
        if not variables:
            raise ValueError("The 'variables' parameter must be a non-empty list.")

        if data_format == DataFormat.CSV and not csv_settings:
            logging.info("'csv_settings' can be configured when data_format is DataFormat.CSV.")

        if data_format == DataFormat.UDBF and not log_settings:
            logging.info("'log_settings' can be configured when data_format is DataFormat.UDBF.")

        payload: Dict[str, Union[str, int, List[Variable], CSVSettings, LogSettings]] = {
            "Start": start,
            "End": end,
            "Variables": variables,
            "Points": points,
            "Type": data_type.value,
            "Format": data_format.value,
            "Precision": precision,
            "TimeZone": timezone,
            "TimeOffset": timeoffset,
        }

        if csv_settings:
            payload["CSVSettings"] = csv_settings

        if log_settings:
            payload["LogSettings"] = log_settings

        if data_format == DataFormat.UDBF and target:
            payload["Target"] = target

        response = requests.post(url, json=payload, headers=self.headers)
        response.raise_for_status()

        if data_format in {DataFormat.COL, DataFormat.ROW, DataFormat.JSON}:
            return response.json()
        else:
            # Results given in bytes
            return response.content

    def get_kafka_raw_structure(self) -> Optional[List[Dict[str, Any]]]:
        """
        Return the list of all Kafka raw streams (“basic success response”).
        Each element looks like
            {
                "ConfigCheckSum": "...",
                "ConfigModTimeSec": 0,
                "DataType": "mtdt" | "data",
                "DeviceID": "-",
                "ID": 1,
                "KafkaTopic": "gi_<type>_t_<tenant>_<SourceID>_<n>",
                "Name": "TekRecording",
                "RetentionTimeSec": 0,
                "SourceID": "<uuid>",
                "StartEpochTimeSec": 0,
                "_id": "41.3"
            }

        Returns
        -------
        list | None
            – on success:   list with one entry per stream
            – on error  :   None (already logged)
        """
        url = f"{self.url}/kafka/raw/structure"
        hdr = {**self.headers, "Content-Type": "application/json"}

        res = requests.get(url, headers=hdr)
        if res.status_code in (401, 403):
            self.refresh_access_token()
            hdr["Authorization"] = f"Bearer {self.login_token['access_token']}"
            res = requests.get(url, headers=hdr)

        if res.ok:
            return res.json().get("Data", [])
        logging.error("get_kafka_raw_structure failed: %s – %s",
                      res.status_code, res.text[:300])
        return None

    def get_kafka_raw_data(
            self,
            kafka_topic: str,
            source_id: str,
            *,
            start_epoch_ms: int = 0,
            end_epoch_ms: Optional[int] = None,
            break_time_gap_ms: Optional[int] = None,
            packet_count: int = 3,
            chunked: bool = True,
            compressed: bool = False,
            timeout: Optional[float] = None,
    ) -> bytes:
        """
        Download raw binary Kafka packets from the server.

        Returns
        -------
        bytes
            Binary response from the Kafka stream (gzip/deflate if compressed=True).
        """
        url = f"{self.url}/kafka/raw/data"
        headers = {
            **self.headers,
            "Content-Type": "application/json",
            "Accept-Encoding": "deflate, gzip",
        }

        payload = {
            "KafkaTopic": kafka_topic,
            "SourceID": source_id,
            "StartEpochTimeMs": start_epoch_ms,
            "PacketCount": packet_count,
            "Chunked": chunked,
            "Compressed": compressed,
        }
        if end_epoch_ms is not None:
            payload["EndEpochTimeMs"] = end_epoch_ms
        if break_time_gap_ms is not None:
            payload["BreakTimeGapMs"] = break_time_gap_ms

        res = requests.post(url, json=payload, headers=headers, timeout=timeout)
        if res.status_code in (401, 403):
            self.refresh_access_token()
            headers["Authorization"] = f"Bearer {self.login_token['access_token']}"
            res = requests.post(url, json=payload, headers=headers, timeout=timeout)

        if not res.ok:
            logging.error("get_kafka_raw_data failed: %s – %s", res.status_code, res.text[:300])
            res.raise_for_status()

        return res.content

    def delete_source(
            self,
            source_id: str,
            *,
            use_kafka: bool = False,
            timeout: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Permanently delete a stream (“source”) on the GI.Cloud backend.

        Parameters
        ----------
        source_id  : str
            UUID of the stream you want to remove.
        use_kafka  : bool, default *False*
            • False  → DELETE /buffer/structure/sources/{SourceID}
            • True   → DELETE /kafka/structure/sources/{SourceID}
        timeout    : float | None
            Optional request timeout in seconds.

        Returns
        -------
        dict | None
            Parsed JSON response on success, otherwise *None* (already logged).

        Notes
        -----
        • The operation is **irreversible** – make sure the stream really should go.
        • A 204/200 status means success even if the backend returns an empty body.
        """
        base = "kafka" if use_kafka else "buffer"
        url = f"{self.url}/{base}/structure/sources/{source_id}"
        hdr = {**self.headers, "Content-Type": "application/json"}

        res = requests.delete(url, json={}, headers=hdr, timeout=timeout)

        if res.status_code in (401, 403):
            self.refresh_access_token()
            hdr["Authorization"] = f"Bearer {self.login_token['access_token']}"
            res = requests.delete(url, json={}, headers=hdr, timeout=timeout)

        if res.ok:
            logging.info("Source %s successfully deleted", source_id)
            try:
                return res.json() if res.text else {}
            except ValueError:
                return {}
        logging.error("delete_source failed for %s: %s – %s",
                      source_id, res.status_code, res.text[:300])
        return None
