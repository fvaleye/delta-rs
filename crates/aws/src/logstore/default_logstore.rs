//! Default implementation of [`LogStore`] for S3 storage backends

use std::sync::Arc;

use bytes::Bytes;
use deltalake_core::logstore::*;
use deltalake_core::{
    kernel::transaction::TransactionError, logstore::ObjectStoreRef, DeltaResult,
};
use object_store::{Error as ObjectStoreError, ObjectStore};
use url::Url;
use uuid::Uuid;

/// Return the [S3LogStore] implementation with the provided configuration options
pub fn default_s3_logstore(
    store: ObjectStoreRef,
    root_store: ObjectStoreRef,
    location: &Url,
    options: &StorageConfig,
) -> Arc<dyn LogStore> {
    Arc::new(S3LogStore::new(
        store,
        root_store,
        LogStoreConfig {
            location: location.clone(),
            options: options.clone(),
        },
    ))
}

/// Default [`LogStore`] implementation
#[derive(Debug, Clone)]
pub struct S3LogStore {
    prefixed_store: ObjectStoreRef,
    root_store: ObjectStoreRef,
    config: LogStoreConfig,
}

impl S3LogStore {
    /// Create a new instance of [`S3LogStore`]
    ///
    /// # Arguments
    ///
    /// * `prefixed_store` - A shared reference to an [`object_store::ObjectStore`]
    ///   with "/" pointing at delta table root (i.e. where `_delta_log` is located).
    /// * `root_store` - A shared reference to an [`object_store::ObjectStore`] with "/"
    ///   pointing at root of the storage system.
    /// * `location` - A url corresponding to the storage location of `storage`.
    pub fn new(
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        config: LogStoreConfig,
    ) -> Self {
        Self {
            prefixed_store,
            root_store,
            config,
        }
    }
}

#[async_trait::async_trait]
impl LogStore for S3LogStore {
    fn name(&self) -> String {
        "S3LogStore".into()
    }

    async fn read_commit_entry(&self, version: i64) -> DeltaResult<Option<Bytes>> {
        read_commit_entry(self.object_store(None).as_ref(), version).await
    }

    /// Tries to commit a prepared commit file. Returns [`TransactionError`]
    /// if the given `version` already exists. The caller should handle the retry logic itself.
    /// This is low-level transaction API. If user does not want to maintain the commit loop then
    /// the `DeltaTransaction.commit` is desired to be used as it handles `try_commit_transaction`
    /// with retry logic.
    async fn write_commit_entry(
        &self,
        version: i64,
        commit_or_bytes: CommitOrBytes,
        _operation_id: Uuid,
    ) -> Result<(), TransactionError> {
        match commit_or_bytes {
            CommitOrBytes::TmpCommit(tmp_commit) => {
                Ok(
                    write_commit_entry(self.object_store(None).as_ref(), version, &tmp_commit)
                        .await?,
                )
            }
            _ => unreachable!(), // S3 Log Store should never receive bytes
        }
        .map_err(|err| -> TransactionError {
            match err {
                ObjectStoreError::AlreadyExists { .. } => {
                    TransactionError::VersionAlreadyExists(version)
                }
                _ => TransactionError::from(err),
            }
        })?;
        Ok(())
    }

    async fn abort_commit_entry(
        &self,
        version: i64,
        commit_or_bytes: CommitOrBytes,
        _operation_id: Uuid,
    ) -> Result<(), TransactionError> {
        match &commit_or_bytes {
            CommitOrBytes::TmpCommit(tmp_commit) => {
                abort_commit_entry(self.object_store(None).as_ref(), version, tmp_commit).await
            }
            _ => unreachable!(), // S3 Log Store should never receive bytes
        }
    }

    async fn get_latest_version(&self, current_version: i64) -> DeltaResult<i64> {
        get_latest_version(self, current_version).await
    }

    fn object_store(&self, _operation_id: Option<Uuid>) -> Arc<dyn ObjectStore> {
        self.prefixed_store.clone()
    }

    fn root_object_store(&self, _operation_id: Option<Uuid>) -> Arc<dyn ObjectStore> {
        self.root_store.clone()
    }

    fn config(&self) -> &LogStoreConfig {
        &self.config
    }
}
