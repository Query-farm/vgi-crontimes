//! Pure cron-projection logic for the `crontimes` VGI worker.
//!
//! This crate is deliberately free of any Arrow/VGI dependency so the cron math
//! can be unit-tested in isolation — this is where correctness lives. The worker
//! crate is a thin adapter that feeds Unix-microsecond timestamps in and reads
//! Unix-microsecond fire times out.
//!
//! All times are handled as `i64` microseconds since the Unix epoch. Firing is
//! computed either in UTC ([`next_fire`]) or in a specific IANA zone for
//! DST-aware schedules ([`next_fire_in`]). Either way the returned value is the
//! UTC instant (micros since epoch); only the worker's output column timezone
//! slot differs between the `TIMESTAMP` and `TIMESTAMPTZ` overloads.

use chrono::{DateTime, Datelike, TimeZone, Utc};
use croner::parser::{CronParser, Seconds};
use croner::Cron;

pub use chrono_tz::Tz;
pub use croner::errors::CronError;

/// Hard upper bound on projection: fire times in this year or later are treated
/// as the end of the series. croner itself supports the calendar up to year 5000;
/// 4096 keeps results comfortably finite for an unbounded query.
pub const YEAR_CAP: i32 = 4096;

/// Parse a cron expression.
///
/// Accepts standard 5-field (`min hour dom month dow`), 6-field (leading
/// `seconds`) and 7-field (trailing `year`) expressions. Seconds are configured
/// `Optional` and croner already treats the year field as optional, so all three
/// field counts parse with this one parser.
pub fn parse_cron(expr: &str) -> Result<Cron, CronError> {
    CronParser::builder()
        .seconds(Seconds::Optional)
        .build()
        .parse(expr)
}

/// The next fire time at/after `from_micros`, computed in **UTC** (no DST).
///
/// * `inclusive == true`  — `from_micros` itself fires if it matches the schedule.
/// * `inclusive == false` — strictly after `from_micros` (use this when stepping
///   the series so the same instant is never emitted twice).
///
/// Returns `None` when there is no further occurrence, when croner reports an
/// error (e.g. an impossible date), or when the next occurrence would land in
/// [`YEAR_CAP`] or later.
pub fn next_fire(cron: &Cron, from_micros: i64, inclusive: bool) -> Option<i64> {
    let from = DateTime::<Utc>::from_timestamp_micros(from_micros)?;
    next_in_zone(cron, &from, inclusive)
}

/// The next fire time at/after `from_micros`, computed in the IANA zone `tz`
/// (**DST-aware**). The cron fields are interpreted as local wall-clock time in
/// `tz`, so e.g. `0 9 * * *` fires at 09:00 local even across daylight-saving
/// transitions; the returned value is still the absolute UTC instant in micros.
///
/// Boundary and termination semantics match [`next_fire`].
pub fn next_fire_in(cron: &Cron, from_micros: i64, inclusive: bool, tz: Tz) -> Option<i64> {
    let from = DateTime::<Utc>::from_timestamp_micros(from_micros)?.with_timezone(&tz);
    next_in_zone(cron, &from, inclusive)
}

/// Shared core: find the next occurrence relative to a zoned datetime and return
/// it as a UTC-instant micros value, applying the [`YEAR_CAP`] cutoff.
fn next_in_zone<Z: TimeZone>(cron: &Cron, from: &DateTime<Z>, inclusive: bool) -> Option<i64> {
    let next = cron.find_next_occurrence(from, inclusive).ok()?;
    if next.year() >= YEAR_CAP {
        return None;
    }
    Some(next.timestamp_micros())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn micros(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> i64 {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, s)
            .unwrap()
            .timestamp_micros()
    }

    #[test]
    fn daily_schedule_is_inclusive_of_a_matching_start() {
        let cron = parse_cron("0 9 * * *").unwrap();
        let start = micros(2026, 6, 18, 9, 0, 0); // exactly 09:00 — a match

        // inclusive => start itself fires.
        let first = next_fire(&cron, start, true).unwrap();
        assert_eq!(first, start);

        // exclusive stepping yields the following days at 09:00.
        let second = next_fire(&cron, first, false).unwrap();
        assert_eq!(second, micros(2026, 6, 19, 9, 0, 0));
        let third = next_fire(&cron, second, false).unwrap();
        assert_eq!(third, micros(2026, 6, 20, 9, 0, 0));
    }

    #[test]
    fn start_between_fires_advances_to_next() {
        let cron = parse_cron("0 9 * * *").unwrap();
        let start = micros(2026, 6, 18, 12, 0, 0); // past 09:00 today
                                                   // Even inclusive, the next match is tomorrow 09:00.
        let next = next_fire(&cron, start, true).unwrap();
        assert_eq!(next, micros(2026, 6, 19, 9, 0, 0));
    }

    #[test]
    fn six_field_seconds_expression_steps_by_thirty_seconds() {
        let cron = parse_cron("*/30 * * * * *").unwrap();
        let start = micros(2026, 6, 18, 9, 0, 0);
        let a = next_fire(&cron, start, true).unwrap();
        assert_eq!(a, start);
        let b = next_fire(&cron, a, false).unwrap();
        assert_eq!(b, micros(2026, 6, 18, 9, 0, 30));
        let c = next_fire(&cron, b, false).unwrap();
        assert_eq!(c, micros(2026, 6, 18, 9, 1, 0));
    }

    #[test]
    fn weekday_only_skips_weekend() {
        // Noon on weekdays (Mon-Fri). 2026-06-19 is a Friday; next is Mon 2026-06-22.
        let cron = parse_cron("0 12 * * 1-5").unwrap();
        let friday_noon = micros(2026, 6, 19, 12, 0, 0);
        let next = next_fire(&cron, friday_noon, false).unwrap();
        assert_eq!(next, micros(2026, 6, 22, 12, 0, 0));
    }

    #[test]
    fn year_cap_terminates_the_series() {
        // Yearly on Jan 1. Stepping from mid-4095 would land on 4096-01-01, which
        // is at/after YEAR_CAP, so the series ends.
        let cron = parse_cron("0 0 1 1 *").unwrap();
        let start = micros(4095, 6, 1, 0, 0, 0);
        assert!(next_fire(&cron, start, false).is_none());
    }

    #[test]
    fn invalid_expression_is_an_error() {
        assert!(parse_cron("not a cron expression").is_err());
        assert!(parse_cron("99 * * * *").is_err());
    }

    const HOUR_US: i64 = 3_600_000_000;

    #[test]
    fn utc_firing_has_no_dst_shift() {
        // Across the US spring-forward weekend, UTC noons stay exactly 24h apart.
        let cron = parse_cron("0 12 * * *").unwrap();
        let f1 = next_fire(&cron, micros(2026, 3, 7, 0, 0, 0), true).unwrap();
        let f2 = next_fire(&cron, f1, false).unwrap();
        assert_eq!(f2 - f1, 24 * HOUR_US);
    }

    #[test]
    fn dst_aware_firing_tracks_local_wall_clock() {
        use chrono_tz::America::New_York;
        // Daily noon in America/New_York. DST begins Sun 2026-03-08, so "noon
        // local" jumps from 17:00Z (EST, UTC-5) to 16:00Z (EDT, UTC-4).
        let cron = parse_cron("0 12 * * *").unwrap();
        let start = New_York
            .with_ymd_and_hms(2026, 3, 7, 0, 0, 0)
            .unwrap()
            .timestamp_micros();

        let f1 = next_fire_in(&cron, start, true, New_York).unwrap();
        assert_eq!(
            f1,
            New_York
                .with_ymd_and_hms(2026, 3, 7, 12, 0, 0)
                .unwrap()
                .timestamp_micros()
        );

        let f2 = next_fire_in(&cron, f1, false, New_York).unwrap();
        assert_eq!(
            f2,
            New_York
                .with_ymd_and_hms(2026, 3, 8, 12, 0, 0)
                .unwrap()
                .timestamp_micros()
        );

        // The smoking gun: consecutive local noons are 23h apart, not 24h,
        // because 2026-03-08 lost an hour to spring-forward.
        assert_eq!(f2 - f1, 23 * HOUR_US);
    }

    #[test]
    fn dst_fall_back_lengthens_the_day() {
        use chrono_tz::America::New_York;
        // Fall-back is Sun 2026-11-01: the local day gains an hour, so noon→noon
        // spans 25h.
        let cron = parse_cron("0 12 * * *").unwrap();
        let start = New_York
            .with_ymd_and_hms(2026, 10, 31, 0, 0, 0)
            .unwrap()
            .timestamp_micros();
        let f1 = next_fire_in(&cron, start, true, New_York).unwrap();
        let f2 = next_fire_in(&cron, f1, false, New_York).unwrap();
        assert_eq!(f2 - f1, 25 * HOUR_US);
    }
}
