//! Clone a Delta table to a new location
//!
//! The clone operation can be either shallow or deep:
//! - Shallow clone: Creates a new table that references the same data files
//! - Deep clone: Creates a new table with copied data files

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use url::Url;

use super::{CustomExecuteHandler, Operation};
use crate::kernel::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::Action;
use crate::logstore::{logstore_for, LogStore, LogStoreRef};
use crate::protocol::DeltaOperation;
use crate::table::builder::ensure_table_uri;
use crate::table::state::DeltaTableState;
use crate::{DeltaResult, DeltaTable, DeltaTableError};
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
                    let absolute_file_uri_str = resolve_absolute_file_uri(
                        file.path.as_str(),
                        &this.source_table_url,
                        this.log_store.as_ref(),
                    )?;

                    updated_file.path = absolute_file_uri_str;
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

/// Resolves a file path to an absolute URI for shallow clone operations
fn resolve_absolute_file_uri(
    orig_path: &str,
    source_table_url: &Url,
    log_store: &dyn LogStore,
) -> DeltaResult<String> {
    // Strategy 1: If already a valid URL with scheme, use as-is
    if let Ok(parsed_url) = Url::parse(orig_path) {
        if !parsed_url.scheme().is_empty() {
            return Ok(orig_path.to_string());
        }
        return source_table_url
            .join(orig_path)
            .map(|url| url.to_string())
            .map_err(|e| {
                DeltaTableError::CloneFailed(format!(
                    "Failed to join '{}' with base '{}' for clone: {}",
                    orig_path, source_table_url, e
                ))
            });
    }

    // Strategy 2: Handle filesystem paths
    let system_path = std::path::Path::new(orig_path);

    if system_path.is_absolute() {
        // Convert absolute filesystem path to file:// URL
        return Url::from_file_path(system_path)
            .map(|url| url.to_string())
            .map_err(|_| {
                DeltaTableError::CloneFailed(format!(
                    "Could not convert absolute OS path to URL for clone: {}",
                    orig_path
                ))
            });
    }

    // Strategy 3: Handle relative paths based on source table URL scheme
    match source_table_url.scheme() {
        "file" => {
            let base_path = source_table_url.to_file_path().map_err(|_| {
                DeltaTableError::CloneFailed(format!(
                    "Source URI '{}' is not a valid local file path for clone.",
                    source_table_url
                ))
            })?;

            let joined_path = base_path.join(orig_path);
            Url::from_file_path(joined_path)
                .map(|url| url.to_string())
                .map_err(|_| {
                    DeltaTableError::CloneFailed(format!(
                        "Could not convert joined path to URL for clone for '{}' relative to '{}'",
                        orig_path, source_table_url
                    ))
                })
        }
        _ => {
            let object_store_path = object_store::path::Path::from(orig_path);
            Ok(log_store.to_uri(&object_store_path))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::create::CreateBuilder;
    use crate::operations::DeltaOps;
    use crate::writer::test_utils::get_delta_schema;
    use std::collections::HashSet;
    use tempfile::TempDir;

    #[test]
    fn test_resolve_absolute_file_uri() {
        use crate::logstore::LogStore;
        use url::Url;

        struct MockLogStore {
            root_uri: String,
        }

        impl LogStore for MockLogStore {
            fn name(&self) -> String {
                "mock".to_string()
            }
            fn root_uri(&self) -> String {
                self.root_uri.clone()
            }
            fn to_uri(&self, path: &object_store::path::Path) -> String {
                format!("s3://bucket/{}", path.as_ref())
            }
            fn config(&self) -> &crate::logstore::LogStoreConfig {
                unimplemented!()
            }
            fn object_store(
                &self,
                _: Option<&crate::logstore::Path>,
            ) -> crate::logstore::ObjectStoreRef {
                unimplemented!()
            }
        }

        let source_url = Url::parse("s3://bucket/table").unwrap();
        let mock_store = MockLogStore {
            root_uri: "s3://bucket/table".to_string(),
        };

        let result =
            resolve_absolute_file_uri("https://example.com/file.parquet", &source_url, &mock_store)
                .unwrap();
        assert_eq!(result, "https://example.com/file.parquet");

        let result =
            resolve_absolute_file_uri("data/file.parquet", &source_url, &mock_store).unwrap();
        assert_eq!(result, "s3://bucket/data/file.parquet");

        let result =
            resolve_absolute_file_uri("part-001.parquet", &source_url, &mock_store).unwrap();
        assert_eq!(result, "s3://bucket/part-001.parquet");
    }

    #[tokio::test]
    async fn test_shallow_clone_should_create_a_new_table_and_reference_the_same_data() {
        let table_schema = get_delta_schema();

        let source_table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .await
            .unwrap();

        let metrics = DeltaOps(source_table)
            .clone()
            .with_target_table_uri("memory:///cloned".to_string())
            .await
            .unwrap();

        assert_eq!(metrics.num_removed_files, 0);
        assert_eq!(metrics.num_copied_files, 0);
        assert!(metrics.source_num_of_files >= 0);
        assert!(metrics.source_table_size >= 0);
    }

    #[tokio::test]
    async fn test_shallow_clone_with_overwrite() {
        let table_schema = get_delta_schema();

        let source_table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .await
            .unwrap();

        // Create target table first at a different location
        let _target_table = DeltaOps::try_from_uri("memory:///target")
            .await
            .unwrap()
            .create()
            .with_columns(table_schema.fields().cloned())
            .await
            .unwrap();

        // Clone with overwrite should succeed
        let metrics = DeltaOps(source_table)
            .clone()
            .with_target_table_uri("memory:///target".to_string())
            .with_replace(true)
            .await
            .unwrap();

        assert_eq!(metrics.num_removed_files, 0); // This test might need adjustment if target had files
        assert_eq!(metrics.num_copied_files, 0);
        assert!(metrics.source_num_of_files >= 0);
        assert!(metrics.source_table_size >= 0);
    }

    #[tokio::test]
    async fn test_shallow_clone_error_if_exists() {
        let table_schema = get_delta_schema();

        let source_table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .await
            .unwrap();

        // Create target table first at a different location
        let _target_table = DeltaOps::try_from_uri("memory:///target2")
            .await
            .unwrap()
            .create()
            .with_columns(table_schema.fields().cloned())
            .await
            .unwrap();

        let result = DeltaOps(source_table)
            .clone()
            .with_target_table_uri("memory:///target2".to_string())
            .await;

        assert!(result.is_err());
        if let Err(DeltaTableError::CloneFailed(msg)) = result {
            assert!(msg.contains("already exists"));
        } else {
            panic!("Expected CloneFailed error");
        }
    }

    #[tokio::test]
    async fn test_shallow_clone_if_not_exists_skips_if_target_exists() {
        let table_schema = get_delta_schema();
        let source_table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .await
            .unwrap();

        // Create target table first
        let target_uri = "memory:///target_if_not_exists";
        let _target_table = DeltaOps::try_from_uri(target_uri)
            .await
            .unwrap()
            .create()
            .with_columns(table_schema.fields().cloned()) // Ensure it has some metadata/version
            .await
            .unwrap();

        let target_delta_table = DeltaOps::try_from_uri(target_uri).await.unwrap();
        let initial_target_version = target_delta_table.version();

        let metrics = DeltaOps(source_table.clone()) // Use clone of source_table
            .clone()
            .with_target_table_uri(target_uri.to_string())
            .with_if_not_exists(true)
            .await
            .unwrap();

        // Verify clone was skipped by checking if target table version is unchanged
        let final_target_table = DeltaOps::try_from_uri(target_uri).await.unwrap();
        assert_eq!(final_target_table.version(), initial_target_version);
        // Metrics should reflect the target table's state as clone was skipped
        let target_files = final_target_table
            .snapshot()
            .unwrap()
            .file_actions()
            .unwrap();
        assert_eq!(metrics.source_num_of_files, target_files.len() as i64);
        assert_eq!(
            metrics.source_table_size,
            target_files.iter().map(|f| f.size).sum()
        );
        assert_eq!(metrics.num_removed_files, 0);
        assert_eq!(metrics.num_copied_files, 0);
    }

    #[tokio::test]
    async fn test_shallow_clone_if_not_exists_proceeds_if_target_does_not_exist() {
        let table_schema = get_delta_schema();
        let source_table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .await
            .unwrap();
        let source_files_count = source_table
            .snapshot()
            .unwrap()
            .file_actions()
            .unwrap()
            .len() as i64;
        let source_size = source_table
            .snapshot()
            .unwrap()
            .file_actions()
            .unwrap()
            .iter()
            .map(|f| f.size)
            .sum();

        let target_uri = "memory:///target_if_not_exists_proceed";
        let metrics = DeltaOps(source_table)
            .clone()
            .with_target_table_uri(target_uri.to_string())
            .with_if_not_exists(true)
            .await
            .unwrap();

        let cloned_table = DeltaOps::try_from_uri(target_uri).await.unwrap();
        assert_eq!(cloned_table.version(), 0); // Cloned table starts at version 0
        assert_eq!(metrics.source_num_of_files, source_files_count);
        assert_eq!(metrics.source_table_size, source_size);
        assert_eq!(metrics.num_removed_files, 0);
        assert_eq!(metrics.num_copied_files, 0);
    }

    #[tokio::test]
    async fn test_shallow_clone_replace_updates_target() {
        let initial_schema = get_delta_schema(); // Schema with 'id' and 'value'
        let mut updated_cols = initial_schema.fields().cloned().collect::<Vec<_>>();
        updated_cols.push(Arc::new(crate::kernel::StructField::new(
            "new_col".to_string(),
            crate::kernel::DataType::STRING,
            true,
        )));

        // Create source table with updated schema
        let source_table = DeltaOps::new_in_memory()
            .create()
            .with_columns(updated_cols.clone())
            .await
            .unwrap();
        let source_snapshot = source_table.snapshot().unwrap();
        let source_files_count = source_snapshot.file_actions().unwrap().len() as i64;
        let source_size = source_snapshot
            .file_actions()
            .unwrap()
            .iter()
            .map(|f| f.size)
            .sum();

        // Create target table with initial schema
        let target_uri = "memory:///target_for_replace";
        let _target_table_initial = DeltaOps::try_from_uri(target_uri)
            .await
            .unwrap()
            .create()
            .with_columns(initial_schema.fields().cloned())
            .await
            .unwrap();

        let target_table_before_replace = DeltaOps::try_from_uri(target_uri).await.unwrap();
        let files_in_target_before_replace = target_table_before_replace
            .snapshot()
            .unwrap()
            .file_actions()
            .unwrap();
        let num_files_in_target_before = files_in_target_before_replace.len() as i64;
        let size_in_target_before: i64 =
            files_in_target_before_replace.iter().map(|f| f.size).sum();

        let metrics = DeltaOps(source_table)
            .clone()
            .with_target_table_uri(target_uri.to_string())
            .with_replace(true)
            .await
            .unwrap();

        let replaced_table = DeltaOps::try_from_uri(target_uri).await.unwrap();
        assert_eq!(
            replaced_table
                .snapshot()
                .unwrap()
                .metadata()
                .schema()
                .fields
                .len(),
            updated_cols.len()
        );
        assert_eq!(metrics.source_num_of_files, source_files_count);
        assert_eq!(metrics.source_table_size, source_size);
        assert_eq!(metrics.num_removed_files, num_files_in_target_before);
        assert_eq!(metrics.removed_files_size, size_in_target_before);
        assert_eq!(metrics.num_copied_files, 0); // Shallow clone
    }

    #[tokio::test]
    async fn test_shallow_clone_should_resolve_file_system_path_correctly() {
        let tmp_dir = TempDir::new().unwrap();
        let source_path = tmp_dir.path().join("source_table");
        let target_path = tmp_dir.path().join("target_table");

        let table_schema = get_delta_schema();
        let source_table = CreateBuilder::new()
            .with_log_store(
                logstore_for(source_path.to_str().unwrap().into(), Default::default()).unwrap(),
            )
            .with_columns(table_schema.fields().cloned())
            .await
            .unwrap();

        // Add a file to the source table to ensure file path resolution is tested
        let data = crate::writer::test_utils::get_record_batch(None, false);
        let mut writer = crate::writer::DeltaWriter::for_table(&source_table).unwrap();
        writer.write(data).await.unwrap();
        writer.commit().await.unwrap();

        let updated_source_table = DeltaTable::try_from_uri(source_path.to_str().unwrap())
            .await
            .unwrap();
        let source_snapshot = updated_source_table.snapshot().unwrap();

        let metrics = CloneBuilder::new(updated_source_table.log_store(), source_snapshot.clone())
            .with_target_table_uri(target_path.to_str().unwrap().to_string())
            .await
            .unwrap();

        assert_eq!(metrics.num_copied_files, 0);
        let target_table = DeltaTable::try_from_uri(target_path.to_str().unwrap())
            .await
            .unwrap();
        let target_files = target_table.snapshot().unwrap().file_actions().unwrap();
        assert_eq!(target_files.len(), 1); // Assuming one file was added

        // Check if the file path in the target table is an absolute URI
        let file_action = target_files.first().unwrap();
        let parsed_url = Url::parse(&file_action.path);
        assert!(
            parsed_url.is_ok(),
            "Path in target table is not a valid URL: {}",
            file_action.path
        );
        assert_eq!(
            parsed_url.unwrap().scheme(),
            "file",
            "Path in target table is not a file URI: {}",
            file_action.path
        );
    }
}
