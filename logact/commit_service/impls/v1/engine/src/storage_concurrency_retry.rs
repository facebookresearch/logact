/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Classification policy for storage concurrency retries.

use agentbus_api::RetryDecision;

use crate::ConcurrencyError;
use crate::StorageError;

#[derive(Default)]
pub(crate) struct ConcurrencyRetryPolicy {
    cas_retry_attempted: bool,
}

impl ConcurrencyRetryPolicy {
    pub(crate) fn decide(&mut self, error: &ConcurrencyError) -> RetryDecision {
        if is_cas_rejection(error) {
            // human: a rejected CAS means a competing write has already
            // completed, so a single immediate retry should reconcile with it.
            if self.cas_retry_attempted {
                return RetryDecision::Stop;
            }
            self.cas_retry_attempted = true;
            return RetryDecision::RetryImmediately;
        }

        if is_transaction_conflict(error) {
            // human: a TxnConflict may be returned before a competing write has
            // completed, so retry with backoff until the configured limit.
            RetryDecision::RetryWithBackoff
        } else {
            RetryDecision::Stop
        }
    }
}

fn is_cas_rejection(error: &ConcurrencyError) -> bool {
    match error {
        ConcurrencyError::Engine { source, .. }
        | ConcurrencyError::Voter { source, .. }
        | ConcurrencyError::Decider { source, .. } => source.is_none(),
    }
}

fn is_transaction_conflict(error: &ConcurrencyError) -> bool {
    match error {
        ConcurrencyError::Engine { source, .. }
        | ConcurrencyError::Voter { source, .. }
        | ConcurrencyError::Decider { source, .. } => {
            matches!(source, Some(StorageError::TransactionConflict(_)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn concurrency_error(source: Option<StorageError>) -> ConcurrencyError {
        ConcurrencyError::Voter {
            bus_id: "bus".to_string(),
            position: 7,
            source,
        }
    }

    #[test]
    fn retries_one_cas_immediately() {
        let mut policy = ConcurrencyRetryPolicy::default();
        let error = concurrency_error(None);

        assert_eq!(policy.decide(&error), RetryDecision::RetryImmediately);
        assert_eq!(policy.decide(&error), RetryDecision::Stop);
    }

    #[test]
    fn transaction_conflict_does_not_restore_cas_retry() {
        let mut policy = ConcurrencyRetryPolicy::default();
        let cas = concurrency_error(None);
        let transaction_conflict = concurrency_error(Some(StorageError::TransactionConflict(
            anyhow::anyhow!("conflict"),
        )));

        assert_eq!(policy.decide(&cas), RetryDecision::RetryImmediately);
        assert_eq!(
            policy.decide(&transaction_conflict),
            RetryDecision::RetryWithBackoff
        );
        assert_eq!(policy.decide(&cas), RetryDecision::Stop);
    }

    #[test]
    fn backs_off_transaction_conflicts() {
        let mut policy = ConcurrencyRetryPolicy::default();
        let error = concurrency_error(Some(StorageError::TransactionConflict(anyhow::anyhow!(
            "conflict"
        ))));

        assert_eq!(policy.decide(&error), RetryDecision::RetryWithBackoff);
    }

    #[test]
    fn stops_on_other_storage_errors() {
        let mut policy = ConcurrencyRetryPolicy::default();
        let error = concurrency_error(Some(StorageError::Timeout(anyhow::anyhow!("timeout"))));

        assert_eq!(policy.decide(&error), RetryDecision::Stop);
    }
}
