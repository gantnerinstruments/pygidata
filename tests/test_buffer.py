from uuid import uuid4

import pandas as pd
import respx

from gi_data.dataclient import GIDataClient


def test_buffer_roundtrip() -> None:
    base = "http://127.0.0.1:8090"
    gid_src = uuid4()
    gid_var = uuid4()

    with respx.mock(base_url=base) as mock:
        mock.get("/buffer/structure/sources").respond(
            json={
                "Success": True,
                "Data": [
                    {
                        "Id": str(gid_src),
                        "Name": "SystemDataStream",
                        "SampleRateHz": 1000,
                    }
                ],
            }
        )

        mock.post("/buffer/data").respond(
            json={
                "Success": True,
                "Data": [
                    {
                        "TimeSeries": {
                            "Type": "equidistant",
                            "Format": "col",
                            "Unit": "ms",
                            "Start": 1_650_000_000_000.0,
                            "AbsoluteStart": 1_650_000_000_000.0,
                            "Delta": 1.0,
                            "End": 1_650_000_000_009.0,
                            "Size": 1,
                            "MeasurementId": str(gid_src),
                            "Updating": False,
                            "Values": [[0.1] * 10],
                        }
                    }
                ],
            }
        )

        client = GIDataClient(base_url=base)
        df = client.fetch_buffer(source_id=gid_src, var_ids=[gid_var])

        assert isinstance(df, pd.DataFrame)
        assert df.shape == (10, 1)
        assert df.iloc[0, 0] == 0.1
