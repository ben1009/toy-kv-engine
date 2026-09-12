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
    fractional_credit: u128,
    pending_stream: Option<(ArchiveStreamId, u64, Instant)>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ArchiveStreamId(pub(crate) [u8; 32]);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StreamGrantOutcome {
    Granted,
    Wait(Duration),
    Busy,
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
                fractional_credit: 0,
                pending_stream: None,
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
        state.pending_stream = None;
        if options.bytes_per_second.is_none() {
            state.fractional_credit = 0;
        }
        Ok(())
    }

    pub(crate) fn try_grant(&self, bytes: NonZeroU64, now: Instant) -> Result<Duration> {
        let mut state = self.state.lock().expect("PITR limiter mutex poisoned");
        ensure!(
            state.pending_stream.is_none(),
            "archive stream reservation is pending"
        );
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
        let wait = duration_for_bytes_with_fraction(deficit, rate.get(), state.fractional_credit);
        Ok(wait)
    }

    pub(crate) fn try_grant_stream(
        &self,
        id: ArchiveStreamId,
        bytes: NonZeroU64,
        now: Instant,
    ) -> Result<StreamGrantOutcome> {
        let mut state = self.state.lock().expect("PITR limiter mutex poisoned");
        let Some(rate) = state.options.bytes_per_second else {
            return Ok(StreamGrantOutcome::Granted);
        };
        let effective_now = now.max(state.last_refill);
        if let Some((pending_id, pending_bytes, ready)) = state.pending_stream {
            if pending_id != id || pending_bytes != bytes.get() {
                return Ok(StreamGrantOutcome::Busy);
            }
            if effective_now >= ready {
                state.pending_stream = None;
                state.tokens = 0;
                state.fractional_credit = 0;
                state.last_refill = ready;
                refill(&mut state, effective_now);
                return Ok(StreamGrantOutcome::Granted);
            }
            return Ok(StreamGrantOutcome::Wait(
                ready.duration_since(effective_now),
            ));
        }
        refill(&mut state, effective_now);
        if bytes.get() <= state.options.burst_bytes.get() {
            if state.tokens < bytes.get() {
                return Ok(StreamGrantOutcome::Wait(duration_for_bytes_with_fraction(
                    bytes.get() - state.tokens,
                    rate.get(),
                    state.fractional_credit,
                )));
            }
            state.tokens -= bytes.get();
            return Ok(StreamGrantOutcome::Granted);
        }
        let wait = duration_for_bytes_with_fraction(
            bytes.get().saturating_sub(state.tokens),
            rate.get(),
            state.fractional_credit,
        );
        let ready = effective_now
            .checked_add(wait)
            .ok_or_else(|| anyhow::anyhow!("archive stream wait exceeds Instant range"))?;
        state.tokens = 0;
        state.fractional_credit = 0;
        state.last_refill = effective_now;
        state.pending_stream = Some((id, bytes.get(), ready));
        Ok(StreamGrantOutcome::Wait(wait))
    }

    pub(crate) fn tokens(&self, now: Instant) -> u64 {
        let mut state = self.state.lock().expect("PITR limiter mutex poisoned");
        refill(&mut state, now);
        state.tokens
    }
}

fn refill(state: &mut LimiterState, now: Instant) {
    let Some(rate) = state.options.bytes_per_second else {
        state.last_refill = now.max(state.last_refill);
        return;
    };
    let capacity = state.options.burst_bytes.get();
    if state.tokens == capacity {
        state.last_refill = now.max(state.last_refill);
        state.fractional_credit = 0;
        return;
    }
    let elapsed = now.saturating_duration_since(state.last_refill);
    let replenished_nanos = elapsed
        .as_nanos()
        .saturating_mul(u128::from(rate.get()))
        .saturating_add(state.fractional_credit);
    let replenished = u64::try_from(replenished_nanos / 1_000_000_000).unwrap_or(u64::MAX);
    let room = capacity - state.tokens;
    if replenished >= room {
        state.tokens = capacity;
        state.last_refill = now.max(state.last_refill);
        state.fractional_credit = 0;
        return;
    }
    state.tokens += replenished;
    state.fractional_credit = replenished_nanos % 1_000_000_000;
    state.last_refill = now.max(state.last_refill);
}

fn duration_for_bytes_with_fraction(bytes: u64, rate: u64, fractional_credit: u128) -> Duration {
    let credited_nanos = fractional_credit;
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

    fn stream_id(value: u8) -> ArchiveStreamId {
        ArchiveStreamId([value; 32])
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
    fn stale_partial_refill_does_not_rewind_time() {
        let start = Instant::now();
        let limiter = PitrArchiveLimiter::new(opts(Some(100), 100), start);
        limiter
            .try_grant(NonZeroU64::new(100).unwrap(), start)
            .unwrap();
        let later = start + Duration::from_secs(1);
        assert_eq!(limiter.tokens(later), 100);
        limiter
            .try_grant(NonZeroU64::new(100).unwrap(), later)
            .unwrap();
        assert_eq!(limiter.tokens(start + Duration::from_millis(500)), 0);
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

    #[test]
    fn stream_grants_support_requests_larger_than_burst() {
        let start = Instant::now();
        let limiter = PitrArchiveLimiter::new(opts(Some(10), 10), start);
        let request = NonZeroU64::new(14).unwrap();
        assert_eq!(
            limiter
                .try_grant_stream(stream_id(1), request, start)
                .unwrap(),
            StreamGrantOutcome::Wait(Duration::from_millis(400))
        );
        assert_eq!(
            limiter
                .try_grant_stream(stream_id(2), request, start)
                .unwrap(),
            StreamGrantOutcome::Busy
        );
        assert_eq!(
            limiter
                .try_grant_stream(stream_id(1), request, start + Duration::from_millis(400))
                .unwrap(),
            StreamGrantOutcome::Granted
        );
    }

    #[test]
    fn late_stream_claim_preserves_post_ready_credit() {
        let start = Instant::now();
        let limiter = PitrArchiveLimiter::new(opts(Some(10), 10), start);
        let request = NonZeroU64::new(14).unwrap();
        assert!(matches!(
            limiter
                .try_grant_stream(stream_id(1), request, start)
                .unwrap(),
            StreamGrantOutcome::Wait(_)
        ));
        let late = start + Duration::from_secs(2);
        assert_eq!(
            limiter
                .try_grant_stream(stream_id(1), request, late)
                .unwrap(),
            StreamGrantOutcome::Granted
        );
        assert_eq!(limiter.tokens(late), 10);
    }

    #[test]
    fn ordinary_grant_cannot_consume_pending_stream_credit() {
        let start = Instant::now();
        let limiter = PitrArchiveLimiter::new(opts(Some(10), 10), start);
        let request = NonZeroU64::new(14).unwrap();
        assert!(matches!(
            limiter
                .try_grant_stream(stream_id(1), request, start)
                .unwrap(),
            StreamGrantOutcome::Wait(_)
        ));
        assert!(
            limiter
                .try_grant(
                    NonZeroU64::new(2).unwrap(),
                    start + Duration::from_millis(200)
                )
                .is_err()
        );
    }

    #[test]
    fn pending_stream_is_cleared_by_runtime_update() {
        let start = Instant::now();
        let limiter = PitrArchiveLimiter::new(opts(Some(10), 10), start);
        let request = NonZeroU64::new(14).unwrap();
        assert!(matches!(
            limiter
                .try_grant_stream(stream_id(1), request, start)
                .unwrap(),
            StreamGrantOutcome::Wait(_)
        ));
        limiter.update(opts(Some(100), 20), start).unwrap();
        assert_eq!(
            limiter
                .try_grant_stream(stream_id(1), request, start)
                .unwrap(),
            StreamGrantOutcome::Wait(Duration::from_millis(140))
        );
    }

    #[test]
    fn unrepresentable_stream_wait_is_rejected() {
        let start = Instant::now();
        let limiter = PitrArchiveLimiter::new(opts(Some(1), 10), start);
        let request = NonZeroU64::new(u64::MAX).unwrap();
        assert!(
            limiter
                .try_grant_stream(stream_id(1), request, start)
                .is_err()
        );
    }

    #[test]
    fn subnanosecond_refill_credit_is_not_reapplied_at_same_timestamp() {
        let start = Instant::now();
        let limiter = PitrArchiveLimiter::new(opts(Some(1_500_000_000), 10), start);
        limiter
            .try_grant(NonZeroU64::new(9).unwrap(), start)
            .unwrap();
        let now = start + Duration::from_nanos(1);
        assert_eq!(limiter.tokens(now), 2);
        assert_eq!(limiter.tokens(now), 2);
    }
}
