
## Package layout
```
┌────────────────────────┐
│  gi_data.dataclient.py │ 
└────────────────────────┘
           ↓
┌────────────────────────────────────────────────────────────┐
│              Driver Factory (selected once)               │
│  • LocalHTTPDriver  – GI.bench / Q.core / Q.station        │
│  • CloudHTTPDriver  – GI.cloud HTTP (same Data-API)        │
│  • CloudGQLDriver   – GI.cloud GraphQL/Kafka-raw           │
│  • WebSocketDriver  – Official workers (shared)           │
└────────────────────────────────────────────────────────────┘
           ↓
┌──────────────┐  ┌────────────────┐  ┌────────────────┐
│ AuthManager  │  │  HttpClient    │  │  WsClient      │ 
└──────────────┘  └────────────────┘  └────────────────┘

```


```gi_data/
├── __init__.py          # re-export GIDataset
├── _version.py
│
├── infra/               # zero domain knowledge
│   ├── auth.py          # refresh, .env, retry, ☑ tokens (shared)
│   ├── http.py          # thin httpx wrapper + retry
│   └── ws.py            # websocket + reconnect
│
├── drivers/             # one sub-package per “product family”
│   ├── base.py          # abstract Driver (online/buffer/history/…)
│   ├── local_http.py    # uses Data-API only
│   ├── cloud_http.py    # same API but cloud login (+Kafka raw)
│   ├── cloud_gql.py     # GraphQL, exportCSV, analytics, …
│   └── ws_shared.py     # OnlineData, SystemState, MessageEvents …
│
├── mapping/             # id<->name caches, pydantic models
│   ├── models.py
│   └── cache.py         # sqlite in ~/.cache/gi_data/<tenant>.db
│
├── dataset.py           # façade class (DSL)
├── analytics.py         # resample, lttb, statistics helpers
└── cli.py               # “gi” command (optional)
tests/  docs/  examples/

```


Q.core: kafka, history, (could be cloud)-gql


### Facade

```
ds = (GIDataset
      .connect("https://tenant.gi-cloud.io", user="u", password="p")
      .with_timezone("Europe/Vienna"))

df = (ds.online()
        .select("Torque", "Speed")      # names or regex!
        .last("10s")
        .as_dataframe())                # unified, regardless of driver
```

| Current snippet                                                      | Where it moves / how it changes                                                                                                      |
| -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `CloudRequest.login/refresh`                                         | `infra.auth.AuthManager` (product-agnostic)                                                                                          |
| `/kafka/structure/sources` parsing                                   | `drivers.cloud_http.list_sources()`                                                                                                  |
| GraphQL `exportCSV`, `analytics`                                     | `drivers.cloud_gql.fetch_history()` (return DataFrame directly, CSV helper lives in analytics)                                       |
| `.env` helpers                                                       | retained but called only inside AuthManager                                                                                          |
| `GIWebSocket`                                                        | replace imperative callbacks with `infra.ws.AsyncWebSocketClient` (asyncio + websockets) but re-use enums & header serializer        |
| `utils.remove_hex_from_string`, etc.                                 | move to `mapping.utils` or delete if unused                                                                                          |
| Long procedural methods (`get_data_as_csv`, `get_var_data_batched`…) | decomposed into: *driver fetch* (returns ndarray/DataFrame) **+** `analytics.to_csv()` helper (handles batching, delimiter, decimal) |
