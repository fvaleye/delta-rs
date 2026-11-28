use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::assert_batches_sorted_eq;
use datafusion::common::Result as DFResult;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::planner::TypePlanner;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion::sql::sqlparser::ast::DataType as SqlDataType;
use deltalake_core::delta_datafusion::create_session;
use deltalake_test::acceptance::{
    Cause, KnownFailure, KnownFailures, ReadSpec, Spec, Workload, check_workload, load_table,
};
use rstest::rstest;
use url::Url;

/// Read workloads where deltalake-core does not do what the spec expects, each with the [`Cause`]
/// at fault. A workload that expects an error is listed only when the read succeeds. None is the
/// executor's: on every workload, its `DataFusionEngine` gives the same result as kernel's
/// default engine (the executor's DAT test checks this).
const KNOWN_FAILURES: KnownFailures = &[
    KnownFailure {
        cause: Cause::Kernel,
        reason: "kernel reads tables that Spark rejects: invalid protocols and corrupt logs",
        workloads: &[
            "checkpoints_checkpoint_only_table_read_all",
            "column_mapping_cm_err_003_invalid_mode_read_all",
            "corruption_err_duplicate_add_same_version_read_all",
            "corruption_err_missing_version_0_read_all",
            "corruption_only_remove_file_read_all",
            "evolvability_fc_empty_json_line_read_all",
            "log_replay_log_err_missing_metadata_read_all",
            "log_replay_log_err_missing_protocol_read_all",
        ],
    },
    KnownFailure {
        cause: Cause::Kernel,
        reason: "kernel allows time travel to a timestamp after the last commit",
        workloads: &[
            "time_travel_future_timestamp_error_read_ts_2099-12-31_23-59-59.999",
            "time_travel_timestamps_read_ts_future",
        ],
    },
    KnownFailure {
        cause: Cause::Kernel,
        reason: "kernel has no fallback to log replay when a checkpoint is missing or incomplete",
        workloads: &[
            "checkpoints_incomplete_multipart_read_all",
            "checkpoints_missing_checkpoint_file_read_all",
            "corruption_wrong_last_checkpoint_read_all",
            "corruption_wrong_last_checkpoint_read_id_lt_5",
        ],
    },
    KnownFailure {
        cause: Cause::Kernel,
        reason: "kernel fails the read on a truncated last line in a commit; Spark skips it",
        workloads: &["corruption_truncated_commit_read_all"],
    },
    KnownFailure {
        cause: Cause::Kernel,
        reason: "kernel reads 2 rows when one commit adds and removes the same path with a \
                 deletion vector; Spark reads 0",
        workloads: &["corruption_err_add_and_remove_same_path_dv_read_all"],
    },
    KnownFailure {
        cause: Cause::Core,
        reason: "deltalake-core allows time travel to a timestamp before the first commit",
        workloads: &["time_travel_timestamps_read_ts_epoch"],
    },
    // Some could be resolved using datafusion-spark extension planner
    KnownFailure {
        cause: Cause::DataFusionSql,
        reason: "DataFusion's SQL lacks a Spark SQL function in the predicate: size, year, month, \
         date_add, date_sub, datediff, trunc, or LIKE on a number",
        workloads: &[
            "check_constraints_array_constraint_read_sizetags_gt_2",
            "data_skipping_date_add_sub_read_d_gte_date_adddate20240101_1",
            "data_skipping_date_add_sub_read_miss_date_sub",
            "data_skipping_datediff_read_datediffd_date20240101_gt_100",
            "data_skipping_datediff_read_datediffd_date20240101_lte_10",
            "data_skipping_month_function_read_miss_month",
            "data_skipping_month_function_read_monthd_eq_1",
            "data_skipping_month_function_read_monthd_eq_6",
            "data_skipping_nulls_only_null_read_like_x",
            "data_skipping_trunc_date_read_miss_trunc",
            "data_skipping_trunc_date_read_truncd_month_eq_date20240301",
            "data_skipping_trunc_date_read_truncd_month_eq_date20240601",
            "data_skipping_year_function_read_miss_year",
            "data_skipping_year_function_read_yeard_eq_2024",
            "data_skipping_year_function_read_yeard_eq_2025",
            "variant_array_variant_filter_array_size",
        ],
    },
];

/// A read workload scans the table through DataFusion and checks the row count, or expects the
/// read to fail.
#[rstest]
#[tokio::test]
async fn scan_dat(
    #[files("../../dat/v0.0.8/*/specs/*.json")]
    #[exclude("snapshot")]
    path: PathBuf,
) {
    let Workload {
        name,
        spec,
        table_root,
    } = Workload::load(&path);
    let Spec::Read(spec) = spec else {
        println!("skipped: not a read workload");
        return;
    };
    check_workload(&name, KNOWN_FAILURES, check(spec, table_root)).await;
}

/// `scan_dat` only counts rows, and no predicate workload reads these widened values, so compare
/// every value with the workload's expected data.
#[rstest]
#[case::date_to_timestamp_ntz("type_widening_date_to_timestamp_ntz")]
#[case::cross_physical_decimal("type_widening_cross_physical_decimal")]
#[tokio::test]
async fn read_widened_values(#[case] table: &str) -> DFResult<()> {
    let spec_path = PathBuf::from(format!(
        "../../dat/v0.0.8/{table}/specs/{table}_read_all.json"
    ));
    let Workload {
        name,
        spec,
        table_root,
    } = Workload::load(&spec_path);
    let Spec::Read(spec) = spec else {
        panic!("{name} is not a read workload");
    };
    let ctx = SessionContext::new();
    let provider = load_table(&table_root, &spec.at)
        .await?
        .table_provider()
        .await?;
    ctx.register_table("t", provider)?;
    let actual = ctx.sql("SELECT * FROM t").await?.collect().await?;

    let expected_data = PathBuf::from(format!(
        "../../dat/v0.0.8/{table}/expected/{name}/expected_data"
    ));
    let expected = ctx
        .read_parquet(
            expected_data.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?
        .collect()
        .await?;
    let expected = pretty_format_batches(&expected)?.to_string();
    assert_batches_sorted_eq!(expected.lines().collect::<Vec<_>>(), &actual);
    Ok(())
}

async fn check(spec: ReadSpec, table_root: Url) -> Result<(), String> {
    let rows = read(&table_root, &spec).await;
    match (rows, &spec.expected) {
        (Ok(rows), Some(expected)) if rows == expected.row_count => Ok(()),
        (Ok(rows), Some(expected)) => Err(format!("{rows} rows, expected {}", expected.row_count)),
        (Err(_), None) => Ok(()),
        (Ok(_), None) => Err("the read succeeds, but the workload expects an error".into()),
        (Err(err), Some(_)) => Err(err.to_string()),
    }
}

/// The number of rows `spec` reads, with its predicate and projection as SQL.
async fn read(table_root: &Url, spec: &ReadSpec) -> datafusion::error::Result<usize> {
    let table = load_table(table_root, &spec.at).await?;
    let ctx: SessionContext = create_session().into_inner();
    let state = SessionStateBuilder::new_from_existing(ctx.state())
        .with_type_planner(Arc::new(SparkTypePlanner))
        .build();
    let ctx = SessionContext::new_with_state(state);
    // Spark resolves column names case-insensitively; keep the names as written.
    ctx.sql("SET datafusion.sql_parser.enable_ident_normalization = false")
        .await?;
    ctx.register_table("t", table.table_provider().await?)?;

    let columns = match &spec.columns {
        Some(columns) => columns
            .iter()
            .map(|column| format!("\"{column}\""))
            .collect::<Vec<_>>()
            .join(", "),
        None => "*".to_string(),
    };
    let filter = match &spec.predicate {
        Some(predicate) => format!(" WHERE {predicate}"),
        None => String::new(),
    };
    let batches = ctx
        .sql(&format!("SELECT {columns} FROM t{filter}"))
        .await?
        .collect()
        .await?;
    Ok(batches.iter().map(|batch| batch.num_rows()).sum())
}

/// Plans the Spark type names that DataFusion's SQL does not know.
#[derive(Debug)]
struct SparkTypePlanner;

impl TypePlanner for SparkTypePlanner {
    fn plan_type_field(&self, sql_type: &SqlDataType) -> DFResult<Option<FieldRef>> {
        let data_type = match sql_type {
            SqlDataType::TimestampNtz(_) => DataType::Timestamp(TimeUnit::Microsecond, None),
            SqlDataType::Custom(name, modifiers) if modifiers.is_empty() => {
                match name.to_string().to_ascii_uppercase().as_str() {
                    "BYTE" => DataType::Int8,
                    "SHORT" => DataType::Int16,
                    _ => return Ok(None),
                }
            }
            _ => return Ok(None),
        };
        Ok(Some(Arc::new(Field::new("", data_type, true))))
    }
}
