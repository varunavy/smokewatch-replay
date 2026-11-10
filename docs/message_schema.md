# SmokeWatch Kafka message schema

Final message format used in the live replay. 18,000 events: 6,000 per source, all `event_id` values unique.

## Topics

| Topic | `source` value | Events |
|---|---|---|
| `firms.events` | `nasa_firms` | 6,000 |
| `epa.pm25.events` | `epa_pm25` | 6,000 |
| `noaa.weather.events` | `noaa_weather` | 6,000 |

Broker: `localhost:9092`. Each topic has 3 partitions, replication factor 1.

## Message key

The county name as UTF-8 text (Butte, Lassen, Plumas, Shasta, Tehama). Events for one county go to the same partition.

## Message value (JSON, UTF-8)

| Field | Description |
|---|---|
| `source` | `nasa_firms`, `epa_pm25` or `noaa_weather` |
| `county` | County name |
| `event_hour` | Hour the data describes, ISO 8601 UTC, e.g. `2021-07-13T01:00:00Z` |
| `event_id` | `<source>\|<county lowercase>\|<YYYYMMDDTHHMMSSZ>`, e.g. `noaa_weather\|butte\|20210713T000000Z` |
| `replay_scenario` | Label for the replay run, e.g. `fast__moderate`. It does not change which events are delayed |
| `delay_hours` | Delay applied to this event: 0.0 for none, 1.0 to 3.0 (moderate), 4.0 to 12.0 (severe) |
| `scheduled_arrival_time` | `event_hour` + `delay_hours`, same ISO format as `event_hour` |
| `payload` | Source-specific fields, below |

## Payload fields common to all sources

| Field | Description |
|---|---|
| `source_sequence` | Sequence number of the event within its source |
| `scheduled_event_time_utc` | Same instant as `event_hour`, written as `2021-07-13 01:00:00+00:00` |
| `source_row_present` | Whether a source row existed for this county-hour |

## `nasa_firms` payload

`fire_detection_count`, `total_frp_mw`, `mean_frp_mw`, `max_frp_mw`, `high_confidence_count`, `night_detection_count`.
`mean_frp_mw` and `max_frp_mw` are null when there are no detections.

## `epa_pm25` payload

`pm25_ug_m3_raw`, `pm25_ug_m3`, `pm25_negative_adjusted`, `monitor_site_count`, `wildfire_flagged_site_count`, `pm25_product`, `pm25_available`.
PM2.5 values are null when `pm25_available` is false.

## `noaa_weather` payload

`weather_station_id`, `weather_station_name`, `station_assignment` (`in_county` or `nearby_proxy`), `weather_observation_count`, `temperature_c`, `relative_humidity_percent`, `wind_speed_m_s`, `maximum_wind_speed_m_s`, `wind_direction_degrees`, `wind_direction_special_count`, `weather_available`, `weather_proxy_used`.
Some counties use a nearby station: `weather_proxy_used` is true there.

## Replay settings

`--speed`: slow = 1, medium = 10, fast = 100 simulated event-hours per real second.
`--delay`: none, moderate (1,846 of 18,000 events delayed), severe (4,549 of 18,000). The same events are delayed at every speed.
