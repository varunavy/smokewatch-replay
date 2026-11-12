# SmokeWatch Replay — system flow (Spark focus)

Same schematic for the written report and slides.

```text
┌────────────┐   ┌────────────┐   ┌────────────┐
│ NASA FIRMS │   │  EPA PM2.5 │   │ NOAA Wx    │
└─────┬──────┘   └─────┬──────┘   └─────┬──────┘
      │                │                │
      ▼                ▼                ▼
┌──────────────────────────────────────────────┐
│ Priya — Kafka replay producer                │
│ topics: firms.events / epa.pm25.events /     │
│         noaa.weather.events @ localhost:9092 │
│ envelope: source, county, event_hour, …      │
└──────────────────────┬───────────────────────┘
                       │
          ┌────────────┴────────────┐
          ▼                         ▼
┌─────────────────────┐   ┌─────────────────────┐
│ Sanjana — MongoDB   │   │ River — Spark SS    │
│ raw event archive   │   │ 3 topic streams →   │
│ (complete envelope) │   │ union → watermark   │
└─────────────────────┘   │ 13h → groupBy       │
                          │ county, event_hour, │
                          │ scenario → join-    │
                          │ ready county-hour   │
                          └──────────┬──────────┘
                                     │ foreachBatch
                                     ▼
                          ┌─────────────────────┐
                          │ Sanjana — Postgres  │
                          │ upsert_county_hour  │
                          │ fact_county_hour +  │
                          │ dims / analysis SQL │
                          └──────────┬──────────┘
                                     ▼
                          ┌─────────────────────┐
                          │ Varuna — batch keys │
                          │ + experiment check  │
                          │ + final package     │
                          └─────────────────────┘
```

**Event-time note:** watermark and freshness use replay/event semantics
(`event_hour`, `scheduled_arrival_time`), not wall-clock age of historical fires.
