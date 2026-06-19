# CLAUDE.md

Guidance for working in this repository.

## What this is

`vgi-crontimes` is a small **example VGI worker**: a standalone Rust binary that DuckDB
launches and talks to over Apache Arrow IPC (`ATTACH 'crontimes' (TYPE vgi, LOCATION '…')`).
It exposes one table function, `cron_fire_times`, under catalog `crontimes`, schema `main`,
which projects the timestamps at which a cron expression will fire from a starting timestamp.

It demonstrates, in a self-contained way: a **table function**, a **polymorphic argument**
(naive `TIMESTAMP` vs `TIMESTAMPTZ`, dispatched at bind), **DST-aware firing** using a
**requested session setting** (`TimeZone`), and a **streaming (multi-batch, bounded-memory)
producer**. Cron math is delegated to the [`croner`](https://crates.io/crates/croner) crate
rather than hand-rolled.

## Layout

- `crates/crontimes-core` — pure cron→micros logic, **no Arrow/VGI deps** (`croner` + `chrono`
  only). Unit-tested directly. This is where correctness lives.
- `crates/crontimes-worker` — thin Arrow/VGI adapter. `main.rs` registers both overloads and
  calls `Worker::run()`; `cron_fire_times.rs` holds the `TableFunction` impl + streaming producer.
- `test/sql/*.test` — SQLLogic tests run via the haybarn unittest runner.
- Depends on the local VGI Rust SDK at `../vgi-rust/vgi` (path dep), arrow 58.

## Build & test

```sh
cargo test -p crontimes-core      # fast cron-math unit tests
cargo build --release             # build the worker -> target/release/crontimes-worker
cargo clippy --all-targets        # keep clean
./run_tests.sh                    # end-to-end SQLLogic suite (needs haybarn, see below)
```

End-to-end tests need the haybarn tooling (one-time):
```sh
uv tool install haybarn-unittest
uv tool install haybarn
echo "INSTALL vgi FROM community;" | uvx haybarn-cli
```

## The function

```
cron_fire_times(cron VARCHAR, start TIMESTAMP|TIMESTAMPTZ, [end := <int|timestamp>])
    -> (seq BIGINT, fire_time TIMESTAMP|TIMESTAMPTZ)
```

- `cron` — 5-field, or 6/7-field with leading seconds / trailing year (croner config).
- `start` — lower bound, polymorphic. A naive `TIMESTAMP` → naive `fire_time`, plain wall-clock
  (no DST). A `TIMESTAMPTZ` → `TIMESTAMPTZ` `fire_time`, fired in the session `TimeZone`
  (DST-aware). An exactly-matching start fires once (inclusive); otherwise the first row is the
  next fire after `start`.
- `end` — optional **named** bound, polymorphic by runtime type:
  - integer ⇒ occurrence count; timestamp ⇒ exclusive upper bound; omitted ⇒ stream to year 4096.
  - `end` is a SQL reserved keyword, so quote it in named-arg position: `"end" := 5`.

## Conventions / gotchas

- The worker catalog name must match the ATTACH name; `main.rs` defaults `VGI_WORKER_CATALOG_NAME`
  to `crontimes`.
- Logs go to **stderr** — stdout is the Arrow-IPC channel.
- Worker user errors (bad cron, bad `end` type) surface to DuckDB as "Invalid Input Error".
- **Why one function, not two literal overloads:** DuckDB encodes the *session* timezone into a
  TIMESTAMPTZ argument's Arrow type (e.g. `Timestamp(us, "America/New_York")`), so a statically
  typed overload declared `Timestamp(us, "UTC")` won't match under a non-UTC session. Instead
  `start` is declared `any` and naive-vs-tz is chosen at bind from the actual Arrow type
  (`start_has_tz`). See the overload scorer in `vgi/src/overload.rs`.
- **DST / TimeZone:** the worker `register_setting("TimeZone")`s so the host forwards DuckDB's
  session zone (resolved via `TryGetCurrentSetting`, populated by ICU). The TIMESTAMPTZ path
  computes in that IANA zone via `chrono-tz` (`crontimes_core::next_fire_in`); missing/non-IANA
  zone falls back to UTC.
- The producer emits in growing batches (2048 → 65536; DuckDB re-chunks, so larger Arrow
  batches just cut IPC round-trips) with bounded memory. An unbounded call streams to the
  year-4096 cap (`'* * * * *'` from 2026 = 1,088,472,960 rows, <10 MB), and DuckDB stops pulling
  once a `LIMIT` is met. Throughput is bound by croner compute, not IPC.
