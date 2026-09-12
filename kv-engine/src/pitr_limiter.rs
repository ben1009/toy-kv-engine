//! Dormant PITR archive I/O token-bucket limiter.
#![allow(dead_code)]

use std::{
    num::NonZeroU64,
    sync::Mutex,
    time::{Duration, Instant},
};

use anyhow::{Result, ensure};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ArchiveLimiterOptions {
    pub(crate) bytes_per_second: Option<NonZeroU64>,
    pub(crate) burst_bytes: NonZeroU64,
}

#[derive(Debug)]
struct LimiterState {
    options: ArchiveLimiterOptions,
    tokens: u64,
    last_refill: Instant,
}

#[derive(Debug)]
pub(crate) struct PitrArchiveLimiter {
    state: Mutex<LimiterState>,
}

impl PitrArchiveLimiter {
    pub(crate) fn new(options: ArchiveLimiterOptions, now: Instant) -> Self {
        Self {
            state: Mutex::new(LimiterState {
                tokens: options.burst_bytes.get(),
                options,
                last_refill: now,
            }),
        }
    }

    pub(crate) fn update(&self, options: ArchiveLimiterOptions, now: Instant) -> Result<()> {
        let mut state = self.state.lock().expect("PITR limiter mutex poisoned");
        let effective_now = now.max(state.last_refill);
        refill(&mut state, effective_now);
        let old_rate = state.options.bytes_per_second;
        state.tokens = match (old_rate, options.bytes_per_second) {
            (Some(_), Some(_)) => state.tokens.min(options.burst_bytes.get()),
            (None, Some(_)) => options.burst_bytes.get(),
            (_, None) => 0,
        };
        state.options = options;
        state.last_refill = effective_now;
        Ok(())
    }

    pub(crate) fn try_grant(&self, bytes: NonZeroU64, now: Instant) -> Result<Duration> {
        let mut state = self.state.lock().expect("PITR limiter mutex poisoned");
        let requested = bytes.get();
        let Some(rate) = state.options.bytes_per_second else {
            return Ok(Duration::ZERO);
        };
        ensure!(
            requested <= state.options.burst_bytes.get(),
            "archive grant exceeds limiter burst"
        );
        refill(&mut state, now);
        if state.tokens >= requested {
            state.tokens -= requested;
            return Ok(Duration::ZERO);
        }
        let deficit = requested - state.tokens;
        let fractional_nanos = now
            .checked_duration_since(state.last_refill)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        let wait = duration_for_bytes_with_fraction(deficit, rate.get(), fractional_nanos);
        Ok(wait)
    }

    pub(crate) fn tokens(&self, now: Instant) -> u64 {
        let mut state = self.state.lock().expect("PITR limiter mutex poisoned");
        refill(&mut state, now);
        state.tokens
    }
}

fn refill(state: &mut LimiterState, now: Instant) {
    let Some(rate) = state.options.bytes_per_second else {
        state.last_refill = now;
        return;
    };
    let capacity = state.options.burst_bytes.get();
    if state.tokens == capacity {
        state.last_refill = now.max(state.last_refill);
        return;
    }
    let elapsed = now.saturating_duration_since(state.last_refill);
    let replenished_nanos = elapsed.as_nanos().saturating_mul(u128::from(rate.get()));
    let replenished = u64::try_from(replenished_nanos / 1_000_000_000).unwrap_or(u64::MAX);
    let room = capacity - state.tokens;
    if replenished >= room {
        state.tokens = capacity;
        state.last_refill = now;
        return;
    }
    state.tokens += replenished;
    if replenished > 0 {
        let consumed_nanos = (u128::from(replenished) * 1_000_000_000)
            .checked_div(u128::from(rate.get()))
            .and_then(|nanos| u64::try_from(nanos).ok())
            .unwrap_or(u64::MAX);
        state.last_refill = state
            .last_refill
            .checked_add(Duration::from_nanos(consumed_nanos))
            .unwrap_or(now);
    }
}

fn duration_for_bytes_with_fraction(bytes: u64, rate: u64, elapsed_nanos: u128) -> Duration {
    let credited_nanos = elapsed_nanos.saturating_mul(u128::from(rate));
    let required_nanos = u128::from(bytes)
        .saturating_mul(1_000_000_000)
        .saturating_sub(credited_nanos);
    let wait_nanos = required_nanos.div_ceil(u128::from(rate));
    duration_from_nanos(wait_nanos)
}

fn duration_from_nanos(nanos: u128) -> Duration {
    let seconds = nanos / 1_000_000_000;
    if seconds > u128::from(u64::MAX) {
        return Duration::MAX;
    }
    Duration::new(seconds as u64, (nanos % 1_000_000_000) as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(rate: Option<u64>, burst: u64) -> ArchiveLimiterOptions {
        ArchiveLimiterOptions {
            bytes_per_second: rate.and_then(NonZeroU64::new),
            burst_bytes: NonZeroU64::new(burst).unwrap(),
        }
    }

    #[test]
    fn starts_full_and_refills_at_rate() {
        let start = Instant::now();
        let limiter = PitrArchiveLimiter::new(opts(Some(100), 200), start);
        assert_eq!(limiter.tokens(start), 200);
        assert_eq!(
            limiter
                .try_grant(NonZeroU64::new(150).unwrap(), start)
                .unwrap(),
            Duration::ZERO
        );
        assert_eq!(limiter.tokens(start + Duration::from_secs(1)), 150);
    }

    #[test]
    fn online_updates_preserve_depleted_tokens_and_handle_unlimited_transitions() {
        let start = Instant::now();
        let limiter = PitrArchiveLimiter::new(opts(Some(100), 100), start);
        limiter
            .try_grant(NonZeroU64::new(80).unwrap(), start)
            .unwrap();
        limiter.update(opts(Some(50), 60), start).unwrap();
        assert_eq!(limiter.tokens(start), 20);
        limiter.update(opts(None, 60), start).unwrap();
        assert_eq!(
            limiter
                .try_grant(NonZeroU64::new(60).unwrap(), start)
                .unwrap(),
            Duration::ZERO
        );
        limiter.update(opts(Some(50), 60), start).unwrap();
        assert_eq!(limiter.tokens(start), 60);
    }

    #[test]
    fn grants_are_bounded_by_burst_and_report_wait() {
        let start = Instant::now();
        let limiter = PitrArchiveLimiter::new(opts(Some(100), 100), start);
        assert!(
            limiter
                .try_grant(NonZeroU64::new(101).unwrap(), start)
                .is_err()
        );
        limiter
            .try_grant(NonZeroU64::new(100).unwrap(), start)
            .unwrap();
        assert_eq!(
            limiter
                .try_grant(NonZeroU64::new(50).unwrap(), start)
                .unwrap(),
            Duration::from_millis(500)
        );
        assert_eq!(
            limiter
                .try_grant(
                    NonZeroU64::new(50).unwrap(),
                    start + Duration::from_millis(500)
                )
                .unwrap(),
            Duration::ZERO
        );
    }

    #[test]
    fn refill_preserves_fractional_time() {
        let start = Instant::now();
        let limiter = PitrArchiveLimiter::new(opts(Some(100), 200), start);
        assert_eq!(limiter.tokens(start + Duration::from_millis(1_005)), 200);
        limiter
            .try_grant(
                NonZeroU64::new(200).unwrap(),
                start + Duration::from_millis(1_005),
            )
            .unwrap();
        assert_eq!(limiter.tokens(start + Duration::from_millis(1_010)), 0);
        assert_eq!(limiter.tokens(start + Duration::from_millis(1_015)), 1);
    }

    #[test]
    fn wait_accounts_for_fractional_credit() {
        let start = Instant::now();
        let limiter = PitrArchiveLimiter::new(opts(Some(100), 100), start);
        limiter
            .try_grant(NonZeroU64::new(100).unwrap(), start)
            .unwrap();
        assert_eq!(
            limiter
                .try_grant(
                    NonZeroU64::new(1).unwrap(),
                    start + Duration::from_millis(5)
                )
                .unwrap(),
            Duration::from_millis(5)
        );
    }

    #[test]
    fn stale_update_does_not_mint_tokens_twice() {
        let start = Instant::now();
        let limiter = PitrArchiveLimiter::new(opts(Some(100), 100), start);
        limiter
            .try_grant(NonZeroU64::new(100).unwrap(), start)
            .unwrap();
        let later = start + Duration::from_secs(1);
        limiter.update(opts(Some(100), 100), later).unwrap();
        limiter
            .try_grant(NonZeroU64::new(100).unwrap(), later)
            .unwrap();
        limiter.update(opts(Some(100), 100), start).unwrap();
        assert_eq!(limiter.tokens(later), 0);
    }

    #[test]
    fn very_large_wait_uses_duration_seconds_range() {
        let start = Instant::now();
        let limiter = PitrArchiveLimiter::new(opts(Some(1), u64::MAX), start);
        limiter
            .try_grant(NonZeroU64::new(u64::MAX).unwrap(), start)
            .unwrap();
        assert_eq!(
            limiter
                .try_grant(NonZeroU64::new(u64::MAX).unwrap(), start)
                .unwrap(),
            Duration::from_secs(u64::MAX)
        );
    }
}
