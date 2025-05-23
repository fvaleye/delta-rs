use deltalake_core::kernel::StructType;
use deltalake_core::operations::DeltaOps;
use serde_json::json;
use std::collections::HashMap;

/// Basic schema for testing
pub fn get_test_schema() -> StructType {
    serde_json::from_value(json!({
      "type": "struct",
      "fields": [
        {"name": "id", "type": "integer", "nullable": false, "metadata": {}},
        {"name": "value", "type": "string", "nullable": true, "metadata": {}},
      ]
    }))
    .unwrap()
}

#[tokio::test]
async fn test_set_table_properties_with_other_config() {
    let table = DeltaOps::new_in_memory()
        .create()
        .with_columns(get_test_schema().fields().cloned())
        .await
        .unwrap();

    let mut properties = HashMap::new();
    properties.insert("delta.enableChangeDataFeed".to_string(), "true".to_string());

    let updated_table = DeltaOps(table)
        .set_tbl_properties()
        .with_properties(properties)
        .await
        .unwrap();

    let metadata = updated_table.metadata().unwrap();
    assert_eq!(
        metadata.configuration.get("delta.enableChangeDataFeed"),
        Some(&Some("true".to_string()))
    );
    assert_eq!(updated_table.version(), 1);
}
