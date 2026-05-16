//! Billing period helpers (Phase D).
//!
//! A period is a (year, month) tuple. The lifecycle is minimal —
//! `Open` (default) or `Closed`. Closed periods reject new `Usage`
//! events at ingest; `Correction` and `Retraction` events for closed
//! periods are still accepted and become post-close adjustments.
//!
//! Future extension points (left for a later PR):
//!   - intermediate states (Closing / Invoiced / Adjusted)
//!   - frozen snapshot semantics (closed-period queries return the
//!     totals as they stood at close-time, not live)
//!   - non-month periods (weekly / quarterly)

use chrono::{DateTime, Datelike};

use crate::storage::manifest::{ClosedPeriod, Manifest};

/// Compute the (year, month) period for an event timestamp. Uses UTC,
/// matching the spec §21 simplification.
pub fn period_for_ts(ts_ms: i64) -> Option<(u16, u8)> {
    let dt = DateTime::from_timestamp_millis(ts_ms)?;
    Some((dt.year() as u16, dt.month() as u8))
}

/// True if the (account, year, month) tuple has been closed.
pub fn is_period_closed(manifest: &Manifest, account: &str, year: u16, month: u8) -> bool {
    manifest
        .closed_periods
        .iter()
        .any(|p| p.account_id == account && p.year == year && p.month == month)
}

/// Parse the `YYYY-MM` URL/CLI param form into (year, month).
pub fn parse_period(s: &str) -> Result<(u16, u8), String> {
    let (year_s, month_s) = s
        .split_once('-')
        .ok_or_else(|| format!("period must be YYYY-MM, got `{}`", s))?;
    let year: u16 = year_s
        .parse()
        .map_err(|e| format!("invalid year `{}`: {}", year_s, e))?;
    let month: u8 = month_s
        .parse()
        .map_err(|e| format!("invalid month `{}`: {}", month_s, e))?;
    if !(1..=12).contains(&month) {
        return Err(format!("month must be 1..=12, got {}", month));
    }
    Ok((year, month))
}

/// Look up the matching ClosedPeriod entry, if any.
pub fn find_closed<'a>(
    manifest: &'a Manifest,
    account: &str,
    year: u16,
    month: u8,
) -> Option<&'a ClosedPeriod> {
    manifest
        .closed_periods
        .iter()
        .find(|p| p.account_id == account && p.year == year && p.month == month)
}
