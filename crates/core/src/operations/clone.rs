//! Clone a Delta table to a new location
//!
//! The clone operation can be either shallow or deep:
//! - Shallow clone: Creates a new table that references the same data files
//! - Deep clone: Creates a new table with copied data files

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use object_store::path::Path;
use url::Url;

use super::{CustomExecuteHandler, Operation};
use crate::kernel::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::Action;
use crate::logstore::{logstore_for, LogStore, LogStoreRef};
use crate::protocol::DeltaOperation;
use crate::table::builder::ensure_table_uri;
use crate::table::state::DeltaTableState;
use crate::{DeltaResult, DeltaTable, DeltaTableError};
use crate::logstore::to_uri;
use uuid;

/// Metrics collected during the clone operation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloneMetrics {
    /// Size of the source table in bytes
    pub source_table_size: i64,
    /// Number of files in the source table
    pub source_num_of_files: i64,
    /// Number of files removed (usually 0 for clone operations)
    pub num_removed_files: i64,
    /// Number of files copied (0 for shallow clones)
    pub num_copied_files: i64,
    /// Size of removed files in bytes (usually 0 for clone operations)
    pub removed_files_size: i64,
    /// Size of copied files in bytes (0 for shallow clones)
    pub copied_files_size: i64,
}

/// Clone a Delta table to a new location
pub struct CloneBuilder {
    /// A snapshot of the source table's state
    snapshot: DeltaTableState,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Source table URI to be cloned
    source_table_url: Url,
    /// Target table URI where the clone will be created
    target_table_uri: String,
    /// Whether to perform a shallow clone (reference files) or deep clone (copy files)
    is_shallow: bool,
    /// If true and the target table exists, the clone operation will be skipped.
    if_not_exists: bool,
    /// Version to clone the table at
    version: Option<i64>,
    /// Timestamp to clone the table at
    timestamp: Option<String>,
    /// If true and the target table exists, it will be replaced.
    replace: bool,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    /// Custom execute handler for pre/post execution
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
    /// Table properties to set on the target table
    tbl_properties: Option<std::collections::HashMap<String, String>>,
}

impl Operation<()> for CloneBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }

    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl CloneBuilder {
    /// Create a new [`CloneBuilder`]
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            snapshot,
            log_store: log_store.clone(),
            source_table_url: log_store.config().location.clone(),
            target_table_uri: String::new(),
            is_shallow: true,
            if_not_exists: false,
            version: None,
            timestamp: None,
            replace: false,
            commit_properties: CommitProperties::default(),
            custom_execute_handler: None,
            tbl_properties: None,
        }
    }

    /// Set the target table URI where the clone will be created
    pub fn with_target_table_uri(mut self, target_table_uri: String) -> Self {
        self.target_table_uri = target_table_uri;
        self
    }

    /// Set whether to perform a shallow clone (default: true)
    /// - Shallow clone: Creates references to existing data files
    /// - Deep clone: Copies data files to the new location
    pub fn with_is_shallow(mut self, is_shallow: bool) -> Self {
        self.is_shallow = is_shallow;
        self
    }

    /// If true and the target table exists, the clone operation will be skipped.
    /// Defaults to `false`. Cannot be true if `replace` is also true.
    pub fn with_if_not_exists(mut self, if_not_exists: bool) -> Self {
        self.if_not_exists = if_not_exists;
        self
    }

    /// If true and the target table exists, it will be replaced by the clone.
    /// Defaults to `false`. Cannot be true if `if_not_exists` is also true.
    pub fn with_replace(mut self, replace: bool) -> Self {
        self.replace = replace;
        self
    }

    /// Additional metadata to be added to commit info
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// Set a custom execute handler for pre and post execution
    pub fn with_custom_execute_handler(mut self, handler: Arc<dyn CustomExecuteHandler>) -> Self {
        self.custom_execute_handler = Some(handler);
        self
    }

    /// Provide a [`LogStore`] instance for the target table
    pub fn with_log_store(mut self, log_store: LogStoreRef) -> Self {
        self.log_store = log_store;
        self
    }

    /// Set table properties for the target table
    pub fn with_tbl_properties(
        mut self,
        tbl_properties: std::collections::HashMap<String, String>,
    ) -> Self {
        self.tbl_properties = Some(tbl_properties);
        self
    }

    /// Set the version to clone the table at
    pub fn with_version(mut self, version: i64) -> Self {
        self.version = Some(version);
        self
    }

    /// Set the timestamp to clone the table at
    pub fn with_timestamp(mut self, timestamp: String) -> Self {
        self.timestamp = Some(timestamp);
        self
    }
}

impl std::future::IntoFuture for CloneBuilder {
    type Output = DeltaResult<CloneMetrics>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let target_url = ensure_table_uri(&this.target_table_uri)?;

            let source_config = this.log_store.config();
            let target_log_store = logstore_for(target_url.clone(), source_config.options.clone())?;

            if this.if_not_exists && this.replace {
                return Err(DeltaTableError::CloneFailed(
                    "if_not_exists and replace flags cannot both be true for clone operation"
                        .to_string(),
                ));
            }

            if this.version.is_some() && this.timestamp.is_some() {
                return Err(DeltaTableError::CloneFailed(
                    "version and timestamp cannot both be provided for clone operation".to_string(),
                ));
            }

            // Determine target disposition and set is_replacing_existing_target
            let mut is_replacing_existing_target = false;
            match target_log_store.is_delta_table_location().await {
                Ok(true) => {
                    // Target is a delta table location
                    if this.if_not_exists {
                        // Target exists and if_not_exists is true. Attempt to load.
                        // If successful, skip clone and return metrics.
                        // If loading fails, error out as we can't safely ignore a corrupted target.
                        let mut target_table =
                            DeltaTable::new(target_log_store.clone(), Default::default());
                        match target_table.load().await {
                            Ok(_) => {
                                let existing_files = target_table.snapshot()?.file_actions()?;
                                let metrics = CloneMetrics {
                                    source_table_size: existing_files
                                        .iter()
                                        .map(|f| f.size)
                                        .sum::<i64>(),
                                    source_num_of_files: existing_files.len() as i64,
                                    num_removed_files: 0,
                                    num_copied_files: 0,
                                    removed_files_size: 0,
                                    copied_files_size: 0,
                                };
                                return Ok(metrics);
                            }
                            Err(e) => {
                                return Err(DeltaTableError::CloneFailed(format!(
                                    "Target table '{}' exists and if_not_exists is true, but could not be loaded: {:?}",
                                    target_url.as_str(),
                                    e
                                )));
                            }
                        }
                    } else if this.replace {
                        // Target exists and replace is true. Proceed with clone.
                        is_replacing_existing_target = true;
                    } else {
                        // Target exists, and neither if_not_exists nor replace is true. This is an error.
                        return Err(DeltaTableError::CloneFailed(format!(
                            "Target table '{}' already exists. Use with_if_not_exists(true) or with_replace(true) to proceed.",
                            target_url.as_str()
                        )));
                    }
                }
                Ok(false) => { // Target path is not a delta table location (or does not exist)
                     // is_replacing_existing_target remains false, proceed with clone.
                }
                Err(e) => {
                    // Error checking target location
                    return Err(DeltaTableError::CloneFailed(format!(
                        "Failed to check status of target location '{}': {:?}. Cannot safely proceed.",
                        target_url.as_str(), e
                    )));
                }
            };

            if this.is_shallow {
                let source_metadata = this.snapshot.metadata().clone();
                let source_protocol = this.snapshot.protocol().clone();
                let source_files = this.snapshot.file_actions()?;

                // Create new metadata with a new ID for the cloned table
                let mut clone_metadata = source_metadata.clone();
                clone_metadata.id = uuid::Uuid::new_v4().to_string();
                // Update the created time to reflect when the clone was created
                clone_metadata.created_time = Some(chrono::Utc::now().timestamp_millis());

                // Apply table properties if provided
                if let Some(properties) = &this.tbl_properties {
                    for (key, value) in properties {
                        clone_metadata
                            .configuration
                            .insert(key.clone(), Some(value.clone()));
                    }
                }

                let mut actions = vec![
                    Action::Protocol(source_protocol.clone()),
                    Action::Metadata(clone_metadata),
                ];

                // For shallow clone, we need to create file references that point directly to the source files
                // using URIs that can be resolved independently of the table they're referenced from
                for file in source_files.iter() {
                    let mut updated_file = file.clone();
                    let mut source_url_for_join = this.source_table_url.clone();
                    if !source_url_for_join.path().ends_with('/') {
                        // Ensure the path ends with a slash to be treated as a directory for join
                        source_url_for_join.path_segments_mut()
                            .map_err(|_| DeltaTableError::CloneFailed("Source URL cannot be a base URL".to_string()))?
                            .push(""); // Appends an empty segment, which results in a trailing slash
                    }
                    let updated_path = source_url_for_join.join(&file.path)
                        .map_err(|e| DeltaTableError::CloneFailed(format!("Failed to join URL for path '{}': {}", file.path, e)))?
                        .to_string();
                    updated_file.path = updated_path;
                    actions.push(Action::Add(updated_file));
                }

                let operation = DeltaOperation::Clone {
                    source_table_uri: this.source_table_url.to_string(),
                    target_table_uri: this.target_table_uri.clone(),
                    shallow: this.is_shallow,
                    location: this.target_table_uri.clone(),
                };

                // Generate a unique operation ID for this clone
                let operation_id = uuid::Uuid::new_v4();
                let mut snapshot_for_commit: Option<crate::kernel::EagerSnapshot> = None;
                let mut actual_num_removed_files: i64 = 0;
                let mut actual_removed_files_size: i64 = 0;

                if is_replacing_existing_target {
                    // load the table at the latest version
                    let mut existing_table =
                        DeltaTable::new(target_log_store.clone(), Default::default());
                    existing_table.load().await?; // Load LATEST state of the target to replace

                    let existing_state = existing_table.snapshot()?;
                    let files_to_remove_from_target = existing_state.file_actions()?;
                    snapshot_for_commit = Some(existing_state.snapshot.clone());

                    actual_num_removed_files = files_to_remove_from_target.len() as i64;
                    actual_removed_files_size = files_to_remove_from_target
                        .iter()
                        .map(|f| f.size)
                        .sum::<i64>();

                    // Add remove actions for existing files
                    let remove_actions = files_to_remove_from_target.iter().map(|f| {
                        Action::Remove(crate::kernel::Remove {
                            path: f.path.clone(),
                            deletion_timestamp: Some(chrono::Utc::now().timestamp_millis()),
                            data_change: true,
                            extended_file_metadata: Some(true),
                            partition_values: Some(f.partition_values.clone()),
                            size: Some(f.size),
                            deletion_vector: f.deletion_vector.clone(),
                            tags: None,
                            base_row_id: f.base_row_id,
                            default_row_commit_version: f.default_row_commit_version,
                        })
                    });
                    actions.extend(remove_actions);
                }

                // Commit the clone
                let _version = CommitBuilder::from(this.commit_properties)
                    .with_actions(actions)
                    .with_operation_id(operation_id)
                    .with_post_commit_hook_handler(this.custom_execute_handler.clone())
                    .build(
                        snapshot_for_commit
                            .as_ref()
                            .map(|s| s as &dyn crate::kernel::transaction::TableReference),
                        target_log_store.clone(),
                        operation,
                    )
                    .await?
                    .version();

                let source_table_size = source_files.iter().map(|f| f.size).sum::<i64>();
                let source_num_of_files = source_files.len() as i64;

                // Shallow clone doesn't copy, just references
                let metrics = CloneMetrics {
                    source_table_size,
                    source_num_of_files,
                    num_removed_files: actual_num_removed_files,
                    num_copied_files: 0,
                    removed_files_size: actual_removed_files_size,
                    copied_files_size: 0,
                };

                Ok(metrics)
            } else {
                // Deep clone is not implemented yet
                Err(DeltaTableError::CloneFailed(
                    "Deep clone is not implemented yet".to_string(),
                ))
            }
        })
    }
}


#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use url::Url;

    use crate::kernel::StructField;
    use crate::operations::create::CreateBuilder;
    use crate::operations::DeltaOps;
    use crate::writer::test_utils::get_record_batch;
    use crate::writer::DeltaWriter;

    use super::*;

    fn get_delta_schema() -> crate::kernel::StructType {
        crate::kernel::StructType::new(vec![
            StructField::new(
                "id".to_string(),
                crate::kernel::DataType::STRING,
                true,
            ),
            StructField::new(
                "value".to_string(),
                crate::kernel::DataType::INTEGER,
                true,
            ),
        ])
    }

    #[tokio::test]
    async fn test_shallow_clone_should_create_a_new_table_and_reference_the_same_data() {
        let tmp_dir = TempDir::new().unwrap();
        let source_path = tmp_dir.path().join("source_table");
        let target_path = tmp_dir.path().join("target_table");
        
        let table_schema = get_delta_schema();

        let source_table = DeltaOps::try_from_uri(&format!("/{}", source_path.to_str().unwrap()))
            .await
            .unwrap()
            .create()
            .with_columns(table_schema.fields().cloned())
            .await
            .unwrap();

        let metrics = DeltaOps(source_table)
            .clone()
            .with_target_table_uri(format!("/{}", target_path.to_str().unwrap()))
            .await
            .unwrap();

        // Verify the target table was created successfully
        let target_table = DeltaOps::try_from_uri(target_path.to_str().unwrap())
            .await
            .unwrap();
        
        let target_snapshot = target_table.0.snapshot().unwrap();
        let target_files = target_snapshot.file_actions().unwrap();

        assert_eq!(metrics.num_removed_files, 0);
        assert_eq!(metrics.num_copied_files, 0);
        assert!(metrics.source_num_of_files >= 0);
        assert!(metrics.source_table_size >= 0);
        
        // Verify that files were referenced (shallow clone)
        assert_eq!(target_files.len() as i64, metrics.source_num_of_files);
        assert_eq!(source_table.snapshot().unwrap().file_actions().unwrap(), target_files);
    }
}
