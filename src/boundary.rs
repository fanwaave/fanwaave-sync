#![forbid(unsafe_code)]

use crate::error::SyncError;
use crate::sqlite::SqliteStore;

/// Maximum bytes admitted by one sync operation, including all batch members.
pub const MAX_SYNC_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug)]
pub struct SyncBoundary {
    pub store: SqliteStore,
    pub enabled: bool,
}

impl SyncBoundary {
    pub fn envelope(&self, bytes: &[u8]) -> Result<usize, SyncError> {
        if !self.enabled {
            return Err(SyncError::Disabled);
        }
        if bytes.len() > MAX_SYNC_BYTES {
            return Err(SyncError::TooLarge);
        }
        Ok(bytes.len())
    }

    /// Preflight an entire batch under the same byte budget as one envelope.
    ///
    /// No payload is committed by this method. Callers must only dispatch a
    /// batch after the whole iterator has been validated, so splitting a large
    /// payload cannot evade the envelope limit.
    pub fn envelopes<'a>(
        &self,
        payloads: impl IntoIterator<Item = &'a [u8]>,
    ) -> Result<usize, SyncError> {
        if !self.enabled {
            return Err(SyncError::Disabled);
        }
        payloads.into_iter().try_fold(0_usize, |total, bytes| {
            let len = self.envelope(bytes)?;
            total
                .checked_add(len)
                .filter(|&sum| sum <= MAX_SYNC_BYTES)
                .ok_or(SyncError::TooLarge)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boundary(enabled: bool) -> SyncBoundary {
        SyncBoundary {
            store: SqliteStore::default(),
            enabled,
        }
    }

    #[test]
    fn accepts_payloads_at_boundary_and_empty_batch() {
        let sync = boundary(true);
        let payload = vec![0_u8; MAX_SYNC_BYTES];
        assert_eq!(sync.envelope(&payload).unwrap(), MAX_SYNC_BYTES);
        assert_eq!(
            sync.envelopes([payload.as_slice()]).unwrap(),
            MAX_SYNC_BYTES
        );
        assert_eq!(sync.envelopes(std::iter::empty()).unwrap(), 0);
    }

    #[test]
    fn rejects_oversized_envelopes_and_aggregate_batches() {
        let sync = boundary(true);
        let oversized = vec![0_u8; MAX_SYNC_BYTES + 1];
        assert!(matches!(
            sync.envelope(&oversized),
            Err(SyncError::TooLarge)
        ));
        assert!(matches!(
            sync.envelopes([oversized.as_slice()]),
            Err(SyncError::TooLarge)
        ));

        let first = vec![0_u8; MAX_SYNC_BYTES];
        let extra = [1_u8];
        assert!(matches!(
            sync.envelopes([first.as_slice(), extra.as_slice()]),
            Err(SyncError::TooLarge)
        ));
        assert_eq!(
            sync.envelopes([&first[..MAX_SYNC_BYTES - 1], extra.as_slice()])
                .unwrap(),
            MAX_SYNC_BYTES
        );
    }

    #[test]
    fn disabled_sync_rejects_even_empty_batches() {
        let sync = boundary(false);
        assert!(matches!(sync.envelope(&[]), Err(SyncError::Disabled)));
        assert!(matches!(
            sync.envelopes(std::iter::empty()),
            Err(SyncError::Disabled)
        ));
    }
}
