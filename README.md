# vgi-crontimes

[![CI](https://github.com/Query-farm/vgi-crontimes/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-crontimes/actions/workflows/ci.yml)

An example [VGI](https://query.farm) worker: a DuckDB **table function** that projects when a
cron expression will fire, starting from a given timestamp. Cron math is provided by the
[`croner`](https://crates.io/crates/croner) crate.

```sql
ATTACH 'crontimes' (TYPE vgi,
  LOCATION '/path/to/vgi-crontimes/target/release/crontimes-worker');

-- the next 5 daily-09:00 fires after a start
SELECT * FROM crontimes.main.cron_fire_times('0 9 * * *', TIMESTAMP '2026-06-18 00:00:00', "end" := 5);
```
```
┌───────┬─────────────────────┐
│  seq  │      fire_time      │
│ int64 │      timestamp      │
├───────┼─────────────────────┤
│   0   │ 2026-06-18 09:00:00 │
│   1   │ 2026-06-19 09:00:00 │
│   2   │ 2026-06-20 09:00:00 │
│   3   │ 2026-06-21 09:00:00 │
│   4   │ 2026-06-22 09:00:00 │
└───────┴─────────────────────┘
```

## Signature

```
cron_fire_times(cron VARCHAR, start TIMESTAMP|TIMESTAMPTZ, [end := <int|timestamp>])
    -> (seq BIGINT, fire_time TIMESTAMP|TIMESTAMPTZ)
```

- **`cron`** — a cron expression. Standard 5-field (`min hour dom month dow`), plus optional
  leading `seconds` (6-field) and/or trailing `year` (7-field).
- **`start`** — the lower bound, and polymorphic: pass a `TIMESTAMP` and `fire_time` comes back
  as a naive `TIMESTAMP` (plain wall-clock, no DST); pass a `TIMESTAMPTZ` and `fire_time` comes
  back as `TIMESTAMPTZ`, fired in the session time zone (**DST-aware** — see below). If `start`
  exactly matches the schedule it fires once (inclusive); otherwise the first row is the next
  fire strictly after `start`.
- **`end`** — optional, **named**, and polymorphic:
  - an **integer** ⇒ return that many occurrences (a count),
  - a **timestamp** ⇒ return every fire time strictly before it,
  - **omitted** ⇒ stream the (effectively infinite) series, hard-capped at year 4096 — pair it
    with `LIMIT`.

  > `end` is a SQL reserved word, so it must be quoted in named-argument position: `"end" := …`.

## DST-aware firing

With a `TIMESTAMPTZ` start, fire times are computed in the session's `TimeZone` (forwarded from
DuckDB; requires the `icu` extension), so wall-clock schedules track daylight-saving:

```sql
LOAD icu;
SET TimeZone = 'America/New_York';
SELECT seq, fire_time, fire_time AT TIME ZONE 'UTC' AS utc_wall
FROM crontimes.main.cron_fire_times('0 12 * * *', TIMESTAMPTZ '2026-03-06', "end" := 4);
```
```
┌─────┬────────────────────────┬─────────────────────┐
│ seq │       fire_time        │      utc_wall       │
├─────┼────────────────────────┼─────────────────────┤
│  0  │ 2026-03-06 12:00:00-05 │ 2026-03-06 17:00:00 │  EST (UTC-5)
│  1  │ 2026-03-07 12:00:00-05 │ 2026-03-07 17:00:00 │
│  2  │ 2026-03-08 12:00:00-04 │ 2026-03-08 16:00:00 │  EDT after spring-forward
│  3  │ 2026-03-09 12:00:00-04 │ 2026-03-09 16:00:00 │
└─────┴────────────────────────┴─────────────────────┘
```

"Noon" stays noon local while the underlying UTC instant shifts an hour. A naive `TIMESTAMP`
start ignores the zone entirely (plain wall-clock). Without `icu`/a session `TimeZone`, the
`TIMESTAMPTZ` path falls back to UTC.

### More examples

```sql
-- every 15 minutes, first 10 (unbounded + LIMIT)
SELECT * FROM crontimes.main.cron_fire_times('*/15 * * * *', TIMESTAMP '2026-06-18 00:00:00') LIMIT 10;

-- weekday noon within a window (timestamp upper bound), tz-aware
SELECT * FROM crontimes.main.cron_fire_times(
    '0 12 * * 1-5',
    TIMESTAMPTZ '2026-06-18 00:00:00+00',
    "end" := TIMESTAMPTZ '2026-07-01 00:00:00+00');

-- 6-field expression with seconds: every 30 seconds
SELECT * FROM crontimes.main.cron_fire_times('*/30 * * * * *', TIMESTAMP '2026-06-18 09:00:00', "end" := 4);
```

## Build

```sh
cargo build --release          # -> target/release/crontimes-worker
cargo test -p crontimes-core   # cron-math unit tests
```

Requires a DuckDB build with the `vgi` extension (e.g. the
[haybarn](https://query.farm) distribution) to `ATTACH` the worker. See `CLAUDE.md` for the
end-to-end test harness.

## How it's built

- `crates/crontimes-core` — pure cron→microseconds logic over `croner` + `chrono`/`chrono-tz`
  (UTC and DST-aware), unit-tested with no Arrow/VGI dependency.
- `crates/crontimes-worker` — the VGI adapter: a `TableFunction` with a streaming
  `TableProducer`. It emits in growing batches (2048 → 65536 rows) with bounded memory, so an
  unbounded query streams to the year-4096 cap without materializing — e.g. `'* * * * *'` from
  2026 yields 1,088,472,960 rows in ~8 min holding <10 MB. (Throughput is bound by cron
  computation, not IPC, so batch size mostly affects round-trip count rather than wall time.)
  The `start` type (naive vs. tz) is dispatched at bind; the session `TimeZone` is requested via
  `register_setting`.
