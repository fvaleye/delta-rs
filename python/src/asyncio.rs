use crate::current_timestamp;
use crate::filesystem;
use crate::rt;
use crate::save_mode_from_str;
use crate::PartitionFilterValue;
use crate::PyAddAction;
use crate::PyArrowType;
use crate::PyDeltaTableError;
use crate::RawDeltaTable;
use crate::RawDeltaTableMetaData;

use chrono::Utc;
use chrono::{DateTime, FixedOffset};
use deltalake::action;
use deltalake::action::{Action, DeltaOperation, SaveMode};
use deltalake::arrow::datatypes::Schema as ArrowSchema;
use deltalake::builder::DeltaTableBuilder;
use deltalake::DeltaTableMetaData;
use deltalake::DeltaTransactionOptions;
use deltalake::Schema;
use pyo3::prelude::*;
use pyo3::types::PyType;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

struct RawAsyncDeltaTableCore {
    mutex: Mutex<RawDeltaTable>,
}

#[pyclass]
pub struct RawAsyncDeltaTable(Arc<RawAsyncDeltaTableCore>);

#[pymethods]
impl RawAsyncDeltaTable {
    #[new]
    fn new(
        table_uri: &str,
        version: Option<deltalake::DeltaDataTypeLong>,
        storage_options: Option<HashMap<String, String>>,
        without_files: bool,
    ) -> PyResult<Self> {
        let mut builder = deltalake::DeltaTableBuilder::from_uri(table_uri);
        if let Some(storage_options) = storage_options {
            builder = builder.with_storage_options(storage_options)
        }
        if let Some(version) = version {
            builder = builder.with_version(version)
        }

        if without_files {
            builder = builder.without_files()
        }

        let table = rt()?
            .block_on(builder.load())
            .map_err(PyDeltaTableError::from_raw)?;
        Ok(RawAsyncDeltaTable(Arc::new(RawAsyncDeltaTableCore {
            mutex: Mutex::new(RawDeltaTable { _table: table }),
        })))
    }

    #[classmethod]
    fn get_table_uri_from_data_catalog<'a>(
        _cls: &PyType,
        data_catalog: String,
        database_name: String,
        table_name: String,
        data_catalog_id: Option<String>,
        py: Python<'a>,
    ) -> PyResult<&'a PyAny> {
        let data_catalog = deltalake::data_catalog::get_data_catalog(&data_catalog)
            .map_err(PyDeltaTableError::from_data_catalog)?;
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let table_uri = data_catalog
                .get_table_storage_location(data_catalog_id, &database_name, &table_name)
                .await
                .map_err(PyDeltaTableError::from_data_catalog)?;

            Ok(table_uri)
        })
    }

    pub fn table_uri(&self) -> PyResult<String> {
        rt()?.block_on(self.0.mutex.lock()).table_uri()
    }

    pub fn version(&self) -> PyResult<i64> {
        rt()?.block_on(self.0.mutex.lock()).version()
    }

    pub fn metadata(&self) -> PyResult<RawDeltaTableMetaData> {
        rt()?.block_on(self.0.mutex.lock()).metadata()
    }

    pub fn protocol_versions(&self) -> PyResult<(i32, i32)> {
        rt()?.block_on(self.0.mutex.lock()).protocol_versions()
    }

    pub fn load_version<'a>(
        &mut self,
        version: deltalake::DeltaDataTypeVersion,
        py: Python<'a>,
    ) -> PyResult<&'a PyAny> {
        let this = Arc::clone(&self.0);
        pyo3_asyncio::tokio::future_into_py(py, async move {
            this.mutex
                .lock()
                .await
                ._table
                .load_version(version)
                .await
                .map_err(PyDeltaTableError::from_raw)?;
            Ok(())
        })
    }

    pub fn load_with_datetime<'a>(&mut self, ds: &str, py: Python<'a>) -> PyResult<&'a PyAny> {
        let this = Arc::clone(&self.0);
        let datetime = DateTime::<Utc>::from(
            DateTime::<FixedOffset>::parse_from_rfc3339(ds)
                .map_err(PyDeltaTableError::from_chrono)?,
        );
        pyo3_asyncio::tokio::future_into_py(py, async move {
            this.mutex
                .lock()
                .await
                ._table
                .load_with_datetime(datetime)
                .await
                .map_err(PyDeltaTableError::from_raw)?;
            Ok(())
        })
    }

    pub fn files_by_partitions(
        &self,
        partitions_filters: Vec<(&str, &str, PartitionFilterValue)>,
    ) -> PyResult<Vec<String>> {
        rt()?
            .block_on(self.0.mutex.lock())
            .files_by_partitions(partitions_filters)
    }

    pub fn files(&self) -> PyResult<Vec<String>> {
        rt()?.block_on(self.0.mutex.lock()).files()
    }

    pub fn file_uris(&self) -> PyResult<Vec<String>> {
        rt()?.block_on(self.0.mutex.lock()).file_uris()
    }

    #[getter]
    pub fn schema(&self, py: Python) -> PyResult<PyObject> {
        rt()?.block_on(self.0.mutex.lock()).schema(py)
    }

    pub fn vacuum(
        &mut self,
        dry_run: bool,
        retention_hours: Option<u64>,
        enforce_retention_duration: bool,
    ) -> PyResult<Vec<String>> {
        rt()?.block_on(self.0.mutex.lock()).vacuum(
            dry_run,
            retention_hours,
            enforce_retention_duration,
        )
    }

    pub fn history<'a>(&mut self, limit: Option<usize>, py: Python<'a>) -> PyResult<&'a PyAny> {
        let this = Arc::clone(&self.0);
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let history = this
                .mutex
                .lock()
                .await
                ._table
                .history(limit)
                .await
                .map_err(PyDeltaTableError::from_raw)?;
            let result: Vec<String> = history
                .iter()
                .map(|c| serde_json::to_string(c).unwrap())
                .collect();
            Ok(result)
        })
    }

    pub fn arrow_schema_json(&self) -> PyResult<String> {
        rt()?.block_on(self.0.mutex.lock()).arrow_schema_json()
    }

    pub fn update_incremental<'a>(&mut self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let this = Arc::clone(&self.0);
        pyo3_asyncio::tokio::future_into_py(py, async move {
            this.mutex
                .lock()
                .await
                ._table
                .update_incremental()
                .await
                .map_err(PyDeltaTableError::from_raw)?;
            Ok(())
        })
    }

    pub fn dataset_partitions<'py>(
        &mut self,
        py: Python<'py>,
        partition_filters: Option<Vec<(&str, &str, PartitionFilterValue)>>,
        schema: PyArrowType<ArrowSchema>,
    ) -> PyResult<Vec<(String, Option<&'py PyAny>)>> {
        rt()?
            .block_on(self.0.mutex.lock())
            .dataset_partitions(py, partition_filters, schema)
    }

    fn create_write_transaction<'a>(
        &mut self,
        add_actions: Vec<PyAddAction>,
        mode: String,
        partition_by: Vec<String>,
        schema: PyArrowType<ArrowSchema>,
        py: Python<'a>,
    ) -> PyResult<&'a PyAny> {
        let this = Arc::clone(&self.0);
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mode = save_mode_from_str(&mode)?;
            let schema: Schema = (&schema.0)
                .try_into()
                .map_err(PyDeltaTableError::from_arrow)?;

            let table = &mut this.mutex.lock().await._table;
            let existing_schema = table
                .get_schema()
                .map_err(PyDeltaTableError::from_raw)?
                .clone();

            let mut actions: Vec<action::Action> = add_actions
                .iter()
                .map(|add| Action::add(add.into()))
                .collect();

            match mode {
                SaveMode::Overwrite => {
                    // Remove all current files
                    for old_add in table.get_state().files().iter() {
                        let remove_action = Action::remove(action::Remove {
                            path: old_add.path.clone(),
                            deletion_timestamp: Some(current_timestamp()),
                            data_change: true,
                            extended_file_metadata: Some(old_add.tags.is_some()),
                            partition_values: Some(old_add.partition_values.clone()),
                            size: Some(old_add.size),
                            tags: old_add.tags.clone(),
                        });
                        actions.push(remove_action);
                    }

                    // Update metadata with new schema
                    if schema != existing_schema {
                        let mut metadata = table
                            .get_metadata()
                            .map_err(PyDeltaTableError::from_raw)?
                            .clone();
                        metadata.schema = schema;
                        let metadata_action =
                            action::MetaData::try_from(metadata).map_err(|_| {
                                PyDeltaTableError::new_err("Failed to reparse metadata")
                            })?;
                        actions.push(Action::metaData(metadata_action));
                    }
                }
                _ => {
                    // This should be unreachable from Python
                    if schema != existing_schema {
                        PyDeltaTableError::new_err("Cannot change schema except in overwrite.");
                    }
                }
            }
            let transaction_options = Some(DeltaTransactionOptions::new(3));

            let mut transaction = table.create_transaction(transaction_options);
            transaction.add_actions(actions);
            transaction
                .commit(
                    Some(DeltaOperation::Write {
                        mode,
                        partition_by: Some(partition_by),
                        predicate: None,
                    }),
                    None,
                )
                .await
                .map_err(PyDeltaTableError::from_raw)?;

            Ok(())
        })
    }

    pub fn get_py_storage_backend(&self) -> PyResult<filesystem::DeltaFileSystemHandler> {
        rt()?.block_on(self.0.mutex.lock()).get_py_storage_backend()
    }
}
