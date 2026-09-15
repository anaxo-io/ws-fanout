//! Initial state for a channel, sent on subscribe.

use std::collections::HashMap;
use std::sync::RwLock;

use serde_json::value::RawValue;

/// Provides the current state of a channel so a new subscriber does not start from
/// nothing and wait for the next delta.
pub trait SnapshotSource: Send + Sync + 'static {
    /// The snapshot for `channel` as JSON, or `None` if the channel has no notion of a
    /// snapshot.
    fn snapshot(&self, channel: &str) -> Option<Box<RawValue>>;
}

/// No snapshots. The default.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoSnapshots;

impl SnapshotSource for NoSnapshots {
    fn snapshot(&self, _: &str) -> Option<Box<RawValue>> {
        None
    }
}

/// Remembers the last payload published on each channel and serves it as the snapshot.
///
/// Right for channels whose latest message *is* the state — a ticker, a full book
/// snapshot. Wrong for channels that publish deltas; those need a real source that holds
/// the assembled state.
#[derive(Debug, Default)]
pub struct LastValue {
    last: RwLock<HashMap<String, Box<RawValue>>>,
}

impl LastValue {
    /// Empty.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `payload` as the latest state of `channel`.
    pub fn record(&self, channel: &str, payload: &RawValue) {
        let mut map = self.last.write().unwrap_or_else(|e| e.into_inner());
        map.insert(channel.to_string(), payload.to_owned());
    }
}

impl SnapshotSource for LastValue {
    fn snapshot(&self, channel: &str) -> Option<Box<RawValue>> {
        self.last
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(channel)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_value_serves_the_latest() {
        let s = LastValue::new();
        assert!(s.snapshot("t").is_none());
        s.record("t", &RawValue::from_string("1".into()).unwrap());
        s.record("t", &RawValue::from_string("2".into()).unwrap());
        assert_eq!(s.snapshot("t").unwrap().get(), "2");
    }
}
