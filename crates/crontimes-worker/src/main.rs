//! The `crontimes` VGI worker.
//!
//! A standalone binary that DuckDB launches and talks to over Apache Arrow IPC
//! (`ATTACH 'crontimes' (TYPE vgi, LOCATION '…')`). It exposes a single table
//! function, `cron_fire_times`, under catalog `crontimes`, schema `main`:
//!
//! ```sql
//! SELECT * FROM crontimes.main.cron_fire_times('0 9 * * *', TIMESTAMP '2026-06-18', "end" := 5);
//! ```
//!
//! See `cron_fire_times.rs` for the table-function implementation and
//! `crontimes-core` for the pure cron math.

mod cron_fire_times;

use vgi::Worker;

fn main() {
    // Logs MUST go to stderr — stdout is the Arrow-IPC channel.
    let _ = env_logger::Builder::from_env(env_logger::Env::default().filter_or("VGI_LOG", "info"))
        .format_timestamp_millis()
        .try_init();

    // The catalog name DuckDB sees in `ATTACH 'crontimes' (TYPE vgi, …)`.
    // Default to `crontimes`, but honor an explicit override so a test harness
    // can rename it.
    if std::env::var_os("VGI_WORKER_CATALOG_NAME").is_none() {
        std::env::set_var("VGI_WORKER_CATALOG_NAME", "crontimes");
    }

    let mut worker = Worker::new();
    cron_fire_times::register(&mut worker);
    worker.run();
}
