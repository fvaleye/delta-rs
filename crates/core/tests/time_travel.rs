use chrono::{DateTime, FixedOffset, Utc};
use std::fs::{FileTimes, OpenOptions};
use std::path::Path;
use std::time::SystemTime;

#[tokio::test]
async fn time_travel_by_ds() {
    // test time travel on a table with an uncommitted delta in a .tmp subfolder

    // git does not preserve mtime, so we need to manually set it in the test
    let log_dir = "../test/tests/data/simple_table/_delta_log";
    let log_mtime_pair = vec![
        ("00000000000000000000.json", "2020-05-01T22:47:31-07:00"),
        ("00000000000000000001.json", "2020-05-02T22:47:31-07:00"),
        ("00000000000000000002.json", "2020-05-03T22:47:31-07:00"),
        ("00000000000000000003.json", "2020-05-04T22:47:31-07:00"),
        ("00000000000000000004.json", "2020-05-05T22:47:31-07:00"),
        // Final file is uncommitted by Spark and is in a .tmp subdir
        (
            ".tmp/00000000000000000005.json",
            "2020-05-06T22:47:31-07:00",
        ),
    ];
    for (fname, ds) in log_mtime_pair {
        let ts: SystemTime = ds_to_ts(ds).into();
        let full_path = Path::new(log_dir).join(fname);
        let file = OpenOptions::new().write(true).open(full_path).unwrap();
        let times = FileTimes::new().set_accessed(ts).set_modified(ts);
        file.set_times(times).unwrap()
    }

    let mut table = deltalake_core::open_table_with_ds(
        "../test/tests/data/simple_table",
        "2020-05-01T00:47:31-07:00",
    )
    .await
    .unwrap();

    assert_eq!(table.version(), Some(0));

    table = deltalake_core::open_table_with_ds(
        "../test/tests/data/simple_table",
        "2020-05-02T22:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), Some(1));

    table = deltalake_core::open_table_with_ds(
        "../test/tests/data/simple_table",
        "2020-05-02T23:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), Some(1));

    table = deltalake_core::open_table_with_ds(
        "../test/tests/data/simple_table",
        "2020-05-03T22:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), Some(2));

    table = deltalake_core::open_table_with_ds(
        "../test/tests/data/simple_table",
        "2020-05-04T22:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), Some(3));

    table = deltalake_core::open_table_with_ds(
        "../test/tests/data/simple_table",
        "2020-05-05T21:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), Some(3));

    table = deltalake_core::open_table_with_ds(
        "../test/tests/data/simple_table",
        "2020-05-05T22:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), Some(4));

    // Final append in .tmp subdir is uncommitted and should be ignored
    table = deltalake_core::open_table_with_ds(
        "../test/tests/data/simple_table",
        "2020-05-25T22:47:31-07:00",
    )
    .await
    .unwrap();
    assert_eq!(table.version(), Some(4));
}

fn ds_to_ts(ds: &str) -> DateTime<Utc> {
    let fixed_dt = DateTime::<FixedOffset>::parse_from_rfc3339(ds).unwrap();
    DateTime::<Utc>::from(fixed_dt)
}
