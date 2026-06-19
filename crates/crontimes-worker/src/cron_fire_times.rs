//! `cron_fire_times(cron, start[, end := …])` — project cron fire times as a table.
//!
//! Output schema: `seq BIGINT, fire_time TIMESTAMP|TIMESTAMPTZ`.
//!
//! `start` is polymorphic: pass a `TIMESTAMP` and `fire_time` comes back as a
//! naive `TIMESTAMP` (plain wall-clock, no DST); pass a `TIMESTAMPTZ` and
//! `fire_time` comes back as `TIMESTAMPTZ`, fired in the session's `TimeZone`
//! (DST-aware). The shape is chosen at bind from the actual `start` Arrow type
//! (a `Timestamp` with a timezone vs. without).
//!
//! A single registered function handles both: DuckDB encodes the *session*
//! timezone into a TIMESTAMPTZ argument's Arrow type, so a pair of statically
//! typed overloads cannot match under an arbitrary session zone — runtime
//! dispatch on the argument type is what works.
//!
//! The optional **named** `end` parameter is polymorphic too:
//!
//! * `"end" := <integer>`   → return that many occurrences (a count).
//! * `"end" := <timestamp>` → return all fire times strictly before that bound.
//! * omitted                → stream the series, capped at year 4096.
//!
//! Note: `end` is a SQL reserved keyword, so the named-arg call must quote it:
//! `cron_fire_times('0 9 * * *', TIMESTAMP '…', "end" := 5)`.

use std::sync::Arc;

use arrow_array::builder::{Int64Builder, TimestampMicrosecondBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::TimestampMicrosecondType;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use croner::Cron;
use vgi::arguments::Arguments;
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableCardinality, TableFunction, TableProducer};
use vgi_rpc::{OutputCollector, Result, RpcError};

/// Batch sizing. DuckDB re-chunks whatever we return, so larger Arrow batches
/// just mean fewer IPC round-trips per row. We start at the standard vector size
/// (cheap first-row latency and minimal overshoot for `LIMIT`/small queries) and
/// double up to [`MAX_BATCH`], so long scans amortize the per-batch overhead.
const MIN_BATCH: usize = 2048;
const MAX_BATCH: usize = 65536;
const TZ_UTC: &str = "UTC";
/// The DuckDB session setting carrying the IANA zone name (set by ICU).
const SETTING_TIMEZONE: &str = "TimeZone";

pub fn register(w: &mut vgi::Worker) {
    // Request DuckDB's session `TimeZone` so a TIMESTAMPTZ start can fire in the
    // user's wall-clock zone (DST-aware). The host resolves this via
    // TryGetCurrentSetting, so it arrives only when ICU provides a TimeZone;
    // absent that, the worker falls back to UTC. Registering the built-in name
    // does not conflict with ICU.
    w.register_setting(vgi::catalog::SettingSpec {
        name: SETTING_TIMEZONE.into(),
        description: "DuckDB session time zone, used for DST-aware TIMESTAMPTZ firing".into(),
        data_type: arrow_schema::DataType::Utf8,
    });
    w.register_table(CronFireTimes);
}

fn err(msg: impl Into<String>) -> RpcError {
    // Worker user errors surface to DuckDB as "Invalid Input Error".
    RpcError::value_error(msg.into())
}

pub struct CronFireTimes;

fn build_schema(with_tz: bool) -> SchemaRef {
    let tz = if with_tz { Some(TZ_UTC.into()) } else { None };
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::Int64, false),
        Field::new(
            "fire_time",
            DataType::Timestamp(TimeUnit::Microsecond, tz),
            false,
        ),
    ]))
}

/// The resolved meaning of the optional `end` argument.
enum End {
    Count(i64),
    Until(i64),
    Unbounded,
}

/// Inspect the polymorphic `end` named arg by its runtime Arrow type.
fn resolve_end(args: &Arguments) -> Result<End> {
    let Some(arr) = args.named("end") else {
        return Ok(End::Unbounded);
    };
    match arr.data_type() {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => {
            let n = args
                .named_i64("end")
                .ok_or_else(|| err("cron_fire_times: 'end' count is not readable"))?;
            if n < 0 {
                return Err(err("cron_fire_times: 'end' count must be non-negative"));
            }
            Ok(End::Count(n))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let micros = arr
                .as_primitive_opt::<TimestampMicrosecondType>()
                .map(|a| a.value(0))
                .ok_or_else(|| err("cron_fire_times: could not read 'end' timestamp"))?;
            Ok(End::Until(micros))
        }
        other => Err(err(format!(
            "cron_fire_times: 'end' must be an integer count or a microsecond timestamp, got {other:?}"
        ))),
    }
}

/// Read the cron expression (positional const arg 0).
fn read_cron(args: &Arguments) -> Result<Cron> {
    let expr = args
        .const_str(0)
        .ok_or_else(|| err("cron_fire_times: a cron expression is required"))?;
    crontimes_core::parse_cron(&expr).map_err(|e| {
        err(format!(
            "cron_fire_times: invalid cron expression '{expr}': {e}"
        ))
    })
}

/// Whether the `start` arg (positional 1) is a TIMESTAMPTZ (`true`) or a naive
/// TIMESTAMP (`false`). This drives the output column type and DST behavior.
fn start_has_tz(args: &Arguments) -> Result<bool> {
    let arr = args
        .arg(1)
        .ok_or_else(|| err("cron_fire_times: a start timestamp is required"))?;
    match arr.data_type() {
        DataType::Timestamp(_, Some(_)) => Ok(true),
        DataType::Timestamp(_, None) => Ok(false),
        other => Err(err(format!(
            "cron_fire_times: 'start' must be a TIMESTAMP or TIMESTAMPTZ, got {other:?}"
        ))),
    }
}

/// Read the start timestamp (positional const arg 1) as Unix microseconds.
fn read_start_micros(args: &Arguments) -> Result<i64> {
    let arr = args
        .arg(1)
        .ok_or_else(|| err("cron_fire_times: a start timestamp is required"))?;
    arr.as_primitive_opt::<TimestampMicrosecondType>()
        .map(|a| a.value(0))
        .ok_or_else(|| err("cron_fire_times: 'start' must be a microsecond timestamp"))
}

/// Resolve the DuckDB session `TimeZone` (forwarded as a setting) to an IANA
/// zone for DST-aware firing. `None` ⇒ compute in UTC (no DST). A session zone
/// that isn't an IANA name (e.g. a fixed offset) also falls back to UTC.
fn resolve_session_tz(settings: &vgi::settings::Settings) -> Option<crontimes_core::Tz> {
    let name = settings.get_str(SETTING_TIMEZONE)?;
    match name.parse::<crontimes_core::Tz>() {
        Ok(tz) => Some(tz),
        Err(_) => {
            log::warn!(
                "cron_fire_times: session TimeZone '{name}' is not an IANA zone; firing in UTC"
            );
            None
        }
    }
}

impl TableFunction for CronFireTimes {
    fn name(&self) -> &str {
        "cron_fire_times"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description:
                "Project the timestamps at which a cron expression will fire, starting from a given timestamp"
                    .to_string(),
            categories: vec!["generator".into()],
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg(
                "cron",
                0,
                "varchar",
                "Cron expression: 5-field, or 6/7-field with leading seconds / trailing year",
            ),
            // `start` is declared ANY so a TIMESTAMP or a TIMESTAMPTZ (whose
            // Arrow type carries the session zone) both bind; the concrete type
            // is inspected at bind to pick the output shape.
            ArgSpec::const_arg(
                "start",
                1,
                "any",
                "Start timestamp (TIMESTAMP or TIMESTAMPTZ); lower bound, an exactly-matching start fires",
            ),
            ArgSpec::const_arg(
                "end",
                -1,
                "any",
                "Optional bound: an integer occurrence count, or a timestamp upper bound. Omitted streams until year 4096.",
            ),
        ]
    }

    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        // Validate eagerly so bad input surfaces at bind, not mid-scan.
        read_cron(&params.arguments)?;
        resolve_end(&params.arguments)?;
        let with_tz = start_has_tz(&params.arguments)?;
        Ok(BindResponse {
            output_schema: build_schema(with_tz),
            opaque_data: Vec::new(),
        })
    }

    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        match resolve_end(&params.arguments).ok()? {
            End::Count(n) => Some(TableCardinality {
                estimate: Some(n),
                max: Some(n),
            }),
            // Unbounded / time-windowed: row count is not known up front.
            _ => None,
        }
    }

    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let cron = read_cron(&params.arguments)?;
        let start = read_start_micros(&params.arguments)?;
        let with_tz = start_has_tz(&params.arguments)?;
        let (remaining, until) = match resolve_end(&params.arguments)? {
            End::Count(n) => (Some(n), None),
            End::Until(u) => (None, Some(u)),
            End::Unbounded => (None, None),
        };
        // DST-aware firing applies only to a TIMESTAMPTZ start (a naive TIMESTAMP
        // has no zone, so it fires in plain wall-clock/UTC).
        let tz = if with_tz {
            resolve_session_tz(&params.settings)
        } else {
            None
        };
        Ok(Box::new(FireProducer {
            cron,
            cursor: start,
            inclusive: true,
            seq: 0,
            remaining,
            until,
            tz,
            batch_size: MIN_BATCH,
            schema: build_schema(with_tz),
            with_tz,
            done: false,
        }))
    }
}

/// Streams fire times in chunks of [`BATCH`], advancing a cursor through the
/// schedule. Stops at the count, the upper-bound timestamp, the year cap, or when
/// the schedule has no further occurrence — whichever comes first.
struct FireProducer {
    cron: Cron,
    /// Last instant considered (advances to each emitted fire time).
    cursor: i64,
    /// True only for the very first lookup, so a start that matches fires once.
    inclusive: bool,
    /// Global 0-based occurrence index, continuous across batches.
    seq: i64,
    /// Remaining occurrences in count mode (`None` = not counting).
    remaining: Option<i64>,
    /// Exclusive upper-bound micros in time-window mode (`None` = no bound).
    until: Option<i64>,
    /// IANA zone to fire in (DST-aware); `None` ⇒ fire in UTC.
    tz: Option<crontimes_core::Tz>,
    /// Rows to emit this call; grows toward [`MAX_BATCH`] across calls.
    batch_size: usize,
    schema: SchemaRef,
    with_tz: bool,
    done: bool,
}

impl TableProducer for FireProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }

        let cap = self.batch_size;
        // Grow the next batch (cheap first batch, big steady-state batches).
        self.batch_size = (self.batch_size * 2).min(MAX_BATCH);

        let mut seqs = Int64Builder::with_capacity(cap);
        let mut fires = TimestampMicrosecondBuilder::with_capacity(cap);
        let mut n = 0usize;

        while n < cap {
            if self.remaining == Some(0) {
                self.done = true;
                break;
            }
            let next = match self.tz {
                Some(tz) => {
                    crontimes_core::next_fire_in(&self.cron, self.cursor, self.inclusive, tz)
                }
                None => crontimes_core::next_fire(&self.cron, self.cursor, self.inclusive),
            };
            let Some(fire) = next else {
                self.done = true;
                break;
            };
            if let Some(u) = self.until {
                if fire >= u {
                    self.done = true;
                    break;
                }
            }

            seqs.append_value(self.seq);
            fires.append_value(fire);

            self.seq += 1;
            self.cursor = fire;
            self.inclusive = false;
            if let Some(r) = self.remaining.as_mut() {
                *r -= 1;
            }
            n += 1;
        }

        if n == 0 {
            return Ok(None);
        }

        // A TIMESTAMPTZ output column carries a "UTC" zone, so the built array's
        // DataType must match it.
        let fire_arr: ArrayRef = if self.with_tz {
            Arc::new(fires.finish().with_timezone(TZ_UTC))
        } else {
            Arc::new(fires.finish())
        };

        let batch =
            RecordBatch::try_new(self.schema.clone(), vec![Arc::new(seqs.finish()), fire_arr])
                .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(Some(batch))
    }
}
