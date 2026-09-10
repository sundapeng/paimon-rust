// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#[path = "../../../paimon/tests/mock_server.rs"]
mod mock_server;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use axum::http::StatusCode;
use paimon::api::ConfigResponse;
use paimon::catalog::RESTCatalog;
use paimon::spec::{BigIntType, BooleanType, DataType, DateType, IntType, Schema, VarCharType};
use paimon::{CatalogOptions, Options};
use paimon_datafusion::SQLContext;
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

use mock_server::{start_mock_server, RESTServer};

const DATABASE: &str = "default";
const TABLE: &str = "events";
const WAREHOUSE: &str = "test_warehouse";

async fn setup_rest_table(temp_dir: &TempDir, schema: Schema) -> (RESTServer, SQLContext) {
    let server = start_mock_server(
        WAREHOUSE.to_string(),
        temp_dir.path().to_string_lossy().into_owned(),
        ConfigResponse::new(HashMap::from([(
            CatalogOptions::PREFIX.to_string(),
            "mock-test".to_string(),
        )])),
        vec![DATABASE.to_string()],
    )
    .await;
    server.add_table_with_schema(
        DATABASE,
        TABLE,
        schema,
        &format!("file://{}", temp_dir.path().display()),
    );
    server.set_table_external(DATABASE, TABLE, false);

    let mut options = Options::new();
    options.set(CatalogOptions::URI, server.url().unwrap());
    options.set(CatalogOptions::WAREHOUSE, WAREHOUSE);
    options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
    options.set(CatalogOptions::TOKEN, "test-token");
    let catalog = Arc::new(RESTCatalog::new(options, true).await.unwrap());
    let mut context = SQLContext::new();
    context.register_catalog("paimon", catalog).await.unwrap();
    (server, context)
}

fn format_table_schema(partition_columns: &[(&str, DataType)]) -> Schema {
    let partition_keys = partition_columns
        .iter()
        .map(|(name, _)| (*name).to_string())
        .collect::<Vec<_>>();
    partition_columns
        .iter()
        .fold(Schema::builder(), |builder, (name, data_type)| {
            builder.column(*name, data_type.clone())
        })
        .column("id", DataType::BigInt(BigIntType::new()))
        .partition_keys(partition_keys)
        .option("type", "format-table")
        .option("file.format", "parquet")
        .option("metastore.partitioned-table", "true")
        .build()
        .unwrap()
}

fn assert_partition_directories(temp_dir: &TempDir, expected: &[(&str, bool)]) {
    for (path, exists) in expected {
        assert_eq!(
            temp_dir.path().join(path).exists(),
            *exists,
            "partition directory {path}"
        );
    }
}

#[cfg(not(windows))]
#[tokio::test]
async fn test_partition_commands_update_rest_metadata_and_directories() {
    let temp_dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(temp_dir.path().join("dt=2026-07-21")).unwrap();
    let schema = format_table_schema(&[("dt", DataType::VarChar(VarCharType::new(255).unwrap()))]);
    let (_server, context) = setup_rest_table(&temp_dir, schema).await;

    context
        .sql("ALTER TABLE paimon.default.events ADD PARTITION (dt = '2026-07-22')")
        .await
        .unwrap();
    assert_partitions(&context, &["dt=2026-07-22"]).await;
    assert_partition_directories(&temp_dir, &[("dt=2026-07-22", true)]);

    context
        .sql("MSCK REPAIR TABLE paimon.default.events ADD PARTITIONS")
        .await
        .unwrap();
    assert_partitions(&context, &["dt=2026-07-21", "dt=2026-07-22"]).await;

    std::fs::remove_dir_all(temp_dir.path().join("dt=2026-07-22")).unwrap();
    context
        .sql("MSCK REPAIR TABLE paimon.default.events SYNC PARTITIONS")
        .await
        .unwrap();
    assert_partitions(&context, &["dt=2026-07-21"]).await;

    context
        .sql("ALTER TABLE paimon.default.events ADD PARTITION (dt = '2026-07-22')")
        .await
        .unwrap();
    context
        .sql("ALTER TABLE paimon.default.events DROP PARTITION (dt = '2026-07-21')")
        .await
        .unwrap();
    context
        .sql("ALTER TABLE paimon.default.events DROP PARTITION (dt = '2026-07-22')")
        .await
        .unwrap();
    assert_partitions(&context, &[]).await;
    assert_partition_directories(
        &temp_dir,
        &[("dt=2026-07-21", false), ("dt=2026-07-22", false)],
    );
}

#[cfg(not(windows))]
#[tokio::test]
async fn test_partition_literals_and_default_path() {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = format_table_schema(&[
        ("dt", DataType::Date(DateType::new())),
        ("month", DataType::Int(IntType::new())),
        ("active", DataType::Boolean(BooleanType::new())),
        ("label", DataType::VarChar(VarCharType::new(255).unwrap())),
    ]);
    let (_server, context) = setup_rest_table(&temp_dir, schema).await;

    context
        .sql(
            "ALTER TABLE paimon.default.events ADD PARTITION (\
             dt = DATE '2026-07-22', month = '01', active = 'TRUE', label = 20260722)",
        )
        .await
        .unwrap();
    assert_partitions(
        &context,
        &["dt=2026-07-22/month=1/active=true/label=20260722"],
    )
    .await;
    // Non-legacy DATE partition paths use Unix epoch days.
    assert_partition_directories(
        &temp_dir,
        &[("dt=20656/month=1/active=true/label=20260722", true)],
    );

    context
        .sql(
            "ALTER TABLE paimon.default.events DROP PARTITION (\
             dt = DATE '2026-07-22', month = '01', active = 'TRUE', label = 20260722)",
        )
        .await
        .unwrap();
    context
        .sql(
            "ALTER TABLE paimon.default.events ADD PARTITION (\
             dt = NULL, month = NULL, active = NULL, label = NULL)",
        )
        .await
        .unwrap();
    assert_partitions(&context, &["dt=null/month=null/active=null/label=null"]).await;
    assert_partition_directories(
        &temp_dir,
        &[(
            "dt=__DEFAULT_PARTITION__/month=__DEFAULT_PARTITION__/\
             active=__DEFAULT_PARTITION__/label=__DEFAULT_PARTITION__",
            true,
        )],
    );
}

#[cfg(not(windows))]
#[tokio::test]
async fn test_drop_partition_takes_several_specs_and_expands_partial_ones() {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = format_table_schema(&[
        ("dt", DataType::VarChar(VarCharType::new(255).unwrap())),
        ("hh", DataType::VarChar(VarCharType::new(255).unwrap())),
    ]);
    let (_server, context) = setup_rest_table(&temp_dir, schema).await;
    add_partitions(&context, &[("20260722", "10"), ("20260722", "11")]).await;

    // A partial spec expands to every registered partition it matches.
    context
        .sql("ALTER TABLE paimon.default.events DROP PARTITION (dt = '20260722')")
        .await
        .unwrap();
    assert_partitions(&context, &[]).await;
    assert_partition_directories(
        &temp_dir,
        &[("dt=20260722/hh=10", false), ("dt=20260722/hh=11", false)],
    );

    // The fixed keys need not be a leading prefix.
    add_partitions(
        &context,
        &[("20260722", "10"), ("20260722", "11"), ("20260723", "10")],
    )
    .await;
    context
        .sql("ALTER TABLE paimon.default.events DROP PARTITION (hh = '10')")
        .await
        .unwrap();
    assert_partitions(&context, &["dt=20260722/hh=11"]).await;
    assert_partition_directories(
        &temp_dir,
        &[
            ("dt=20260722/hh=10", false),
            ("dt=20260723/hh=10", false),
            ("dt=20260722/hh=11", true),
        ],
    );

    // One statement may carry several specifications.
    add_partitions(&context, &[("20260723", "10"), ("20260724", "12")]).await;
    context
        .sql(
            "ALTER TABLE paimon.default.events \
             DROP PARTITION (dt = '20260722', hh = '11'), DROP PARTITION (dt = '20260723')",
        )
        .await
        .unwrap();
    assert_partitions(&context, &["dt=20260724/hh=12"]).await;
}

#[cfg(not(windows))]
#[tokio::test]
async fn test_drop_partition_reports_a_specification_that_matches_nothing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = format_table_schema(&[
        ("dt", DataType::VarChar(VarCharType::new(255).unwrap())),
        ("hh", DataType::VarChar(VarCharType::new(255).unwrap())),
    ]);
    let (_server, context) = setup_rest_table(&temp_dir, schema).await;
    add_partitions(&context, &[("20260722", "10")]).await;

    // A complete specification names one partition, so a missing one is an error.
    let error = context
        .sql("ALTER TABLE paimon.default.events DROP PARTITION (dt = '20260723', hh = '10')")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("does not exist"), "{error}");
    assert_partitions(&context, &["dt=20260722/hh=10"]).await;

    context
        .sql(
            "ALTER TABLE paimon.default.events \
             DROP IF EXISTS PARTITION (dt = '20260723', hh = '10')",
        )
        .await
        .unwrap();
    assert_partitions(&context, &["dt=20260722/hh=10"]).await;

    // A partial specification describes a set that is allowed to come out empty, so it is
    // a no-op rather than an error, and it does not take the statement down with it.
    context
        .sql(
            "ALTER TABLE paimon.default.events \
             DROP PARTITION (dt = '20260723'), DROP PARTITION (dt = '20260722')",
        )
        .await
        .unwrap();
    assert_partitions(&context, &[]).await;

    // One failing specification must leave the whole statement unapplied.
    add_partitions(&context, &[("20260722", "10")]).await;
    let error = context
        .sql(
            "ALTER TABLE paimon.default.events \
             DROP PARTITION (dt = '20260722', hh = '10'), \
             DROP PARTITION (dt = '20260723', hh = '10')",
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("does not exist"), "{error}");
    assert_partitions(&context, &["dt=20260722/hh=10"]).await;

    // Dropping partitions cannot be combined with a schema change.
    let error = context
        .sql("ALTER TABLE paimon.default.events DROP PARTITION (dt = '20260722'), ADD COLUMN c INT")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("must be used alone"), "{error}");
    assert_partitions(&context, &["dt=20260722/hh=10"]).await;
}

#[cfg(not(windows))]
// Planning a SELECT resolves the table on a blocking catalog-access thread, so the mock
// server needs a runtime thread of its own to answer while that one waits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_catalog_managed_scan_pushes_a_partition_name_pattern() {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = format_table_schema(&[
        ("dt", DataType::VarChar(VarCharType::new(255).unwrap())),
        ("hh", DataType::VarChar(VarCharType::new(255).unwrap())),
    ]);
    let (server, context) = setup_rest_table(&temp_dir, schema).await;
    add_partitions(
        &context,
        &[("20260722", "10"), ("20260722", "11"), ("20260723", "10")],
    )
    .await;

    for (predicate, expected) in [
        ("dt = '20260722' AND hh = '10'", Some("dt=20260722/hh=10")),
        ("dt = '20260722'", Some("dt=20260722/%")),
        (
            "dt = '20260722' AND hh IN ('10', '11')",
            Some("dt=20260722/%"),
        ),
        // Only a leading run of equalities becomes a prefix pattern; anything else has to
        // list every partition and prune locally.
        ("hh = '10'", None),
        ("dt > '20260722'", None),
    ] {
        let seen = listing_counts(&server);
        context
            .sql(&format!(
                "SELECT * FROM paimon.default.events WHERE {predicate}"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let pushed = name_patterns_since(&server, seen);
        assert!(!pushed.is_empty(), "{predicate} listed no partitions");
        assert!(
            pushed.iter().all(|pattern| pattern.as_deref() == expected),
            "{predicate} pushed {pushed:?}, expected {expected:?}"
        );
    }
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_catalog_managed_scan_sends_its_partition_predicate_as_a_filter() {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = format_table_schema(&[
        ("dt", DataType::VarChar(VarCharType::new(255).unwrap())),
        ("hh", DataType::VarChar(VarCharType::new(255).unwrap())),
    ]);
    let (server, context) = setup_rest_table(&temp_dir, schema).await;
    for (dt, hh, id) in [
        ("20260722", "10", 1),
        ("20260722", "11", 2),
        ("20260723", "10", 3),
    ] {
        write_ids(&temp_dir.path().join(format!("dt={dt}/hh={hh}")), &[id]);
    }
    add_partitions(
        &context,
        &[("20260722", "10"), ("20260722", "11"), ("20260723", "10")],
    )
    .await;

    // No leading equality, so only the filter can narrow what the catalog returns.
    assert_eq!(
        ids(
            &context,
            "SELECT id FROM paimon.default.events WHERE hh = '10'"
        )
        .await,
        vec![1, 3]
    );
    let requests = server.table_partition_list_by_filter_requests(DATABASE, TABLE);
    let request = requests.last().expect("the scan should list by filter");
    assert_eq!(request.partition_name_pattern, None);
    assert_eq!(request.max_results, Some(1000));
    let filter: serde_json::Value = serde_json::from_str(&request.filter).unwrap();
    assert_eq!(filter["function"], "EQUAL");
    assert_eq!(filter["transform"]["fieldRef"]["name"], "hh");
    assert_eq!(filter["transform"]["fieldRef"]["index"], 1);
    assert_eq!(filter["literals"], serde_json::json!(["10"]));

    // A catalog that cannot list by filter is still asked, by pattern; the partition set never
    // comes from the directory tree.
    server.set_list_partitions_by_filter_error_status(Some(StatusCode::NOT_IMPLEMENTED));
    let listed = server
        .table_partition_list_name_patterns(DATABASE, TABLE)
        .len();
    assert_eq!(
        ids(
            &context,
            "SELECT id FROM paimon.default.events WHERE dt = '20260722' AND hh > '10'"
        )
        .await,
        vec![2]
    );
    assert_eq!(
        server.table_partition_list_name_patterns(DATABASE, TABLE)[listed..],
        [Some("dt=20260722/%".to_string())]
    );
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_catalog_managed_scan_keeps_registrations_spelled_unlike_the_filter() {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = format_table_schema(&[
        ("month", DataType::Int(IntType::new())),
        ("active", DataType::Boolean(BooleanType::new())),
    ]);
    let (server, context) = setup_rest_table(&temp_dir, schema).await;
    // Repair registers directory values as they are, so the catalog can hold spellings that a
    // typed literal never formats to.
    write_ids(&temp_dir.path().join("month=01/active=TRUE"), &[1]);
    write_ids(&temp_dir.path().join("month=2/active=false"), &[2]);
    server.set_table_partitions(
        DATABASE,
        TABLE,
        vec![
            spec(&[("month", "01"), ("active", "TRUE")]),
            spec(&[("month", "2"), ("active", "false")]),
        ],
    );

    for (predicate, expected) in [
        ("month = 1", vec![1]),
        ("month = 1 AND active = true", vec![1]),
        ("month = 2 AND active = false", vec![2]),
    ] {
        assert_eq!(
            ids(
                &context,
                &format!("SELECT id FROM paimon.default.events WHERE {predicate}")
            )
            .await,
            expected,
            "{predicate}"
        );
    }
    // A pattern built from `month = 1` would have dropped `month=01` on the catalog side.
    assert!(name_patterns_since(&server, (0, 0))
        .iter()
        .all(Option::is_none));
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_format_table_filter_on_a_partition_column_reads_the_directory_value() {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = format_table_schema(&[
        ("dt", DataType::VarChar(VarCharType::new(255).unwrap())),
        ("active", DataType::Boolean(BooleanType::new())),
    ]);
    let (_server, context) = setup_rest_table(&temp_dir, schema).await;
    for (dt, active, id) in [("a", true, 1), ("b", false, 2)] {
        context
            .sql(&format!(
                "ALTER TABLE paimon.default.events ADD PARTITION (dt = '{dt}', active = {active})"
            ))
            .await
            .unwrap();
        write_ids(
            &temp_dir.path().join(format!("dt={dt}/active={active}")),
            &[id],
        );
    }

    // The data files hold no partition columns. A filter the scan cannot turn into a partition
    // predicate still has to see the value from the directory name, not a missing column.
    for (predicate, expected) in [
        ("active", vec![1]),
        ("NOT active", vec![2]),
        ("upper(dt) = 'B'", vec![2]),
        ("concat(dt, '-') = 'a-'", vec![1]),
    ] {
        assert_eq!(
            ids(
                &context,
                &format!("SELECT id FROM paimon.default.events WHERE {predicate}")
            )
            .await,
            expected,
            "{predicate}"
        );
    }
}

#[cfg(not(windows))]
#[tokio::test]
async fn test_drop_partition_matches_values_as_the_catalog_holds_them() {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = format_table_schema(&[
        ("year", DataType::VarChar(VarCharType::new(255).unwrap())),
        ("month", DataType::Int(IntType::new())),
    ]);
    let (server, context) = setup_rest_table(&temp_dir, schema).await;
    // Repair keeps directory spellings, so both registrations are legitimate and distinct.
    for directory in ["year=2025/month=01", "year=2026/month=1"] {
        std::fs::create_dir_all(temp_dir.path().join(directory)).unwrap();
    }
    server.set_table_partitions(
        DATABASE,
        TABLE,
        vec![
            spec(&[("year", "2025"), ("month", "01")]),
            spec(&[("year", "2026"), ("month", "1")]),
        ],
    );

    // A request is spelled the way ADD PARTITION registers it, so `month = 1` is not `month=01`.
    let error = context
        .sql("ALTER TABLE paimon.default.events DROP PARTITION (year = '2025', month = 1)")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("does not exist"), "{error}");
    context
        .sql(
            "ALTER TABLE paimon.default.events DROP IF EXISTS PARTITION (year = '2025', month = 1)",
        )
        .await
        .unwrap();

    context
        .sql("ALTER TABLE paimon.default.events DROP PARTITION (month = 1)")
        .await
        .unwrap();
    assert_eq!(
        server.table_partition_specs(DATABASE, TABLE),
        vec![spec(&[("year", "2025"), ("month", "01")])]
    );
    assert_partition_directories(
        &temp_dir,
        &[("year=2025/month=01", true), ("year=2026/month=1", false)],
    );
}

#[cfg(not(windows))]
#[tokio::test]
async fn test_drop_partition_looks_up_complete_specifications_by_name() {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = format_table_schema(&[
        ("dt", DataType::VarChar(VarCharType::new(255).unwrap())),
        ("hh", DataType::VarChar(VarCharType::new(255).unwrap())),
    ]);
    let (server, context) = setup_rest_table(&temp_dir, schema).await;
    add_partitions(
        &context,
        &[("20260722", "10"), ("20260722", "11"), ("20260723", "10")],
    )
    .await;
    let listings = server
        .table_partition_list_name_patterns(DATABASE, TABLE)
        .len();

    context
        .sql(
            "ALTER TABLE paimon.default.events \
             DROP PARTITION (dt = '20260722', hh = '10'), DROP PARTITION (dt = '20260723', hh = '10')",
        )
        .await
        .unwrap();
    assert_eq!(
        server
            .table_partition_list_name_patterns(DATABASE, TABLE)
            .len(),
        listings,
        "complete specifications should not read the registry"
    );
    assert_eq!(
        server.table_partition_list_by_names_calls(DATABASE, TABLE),
        vec![vec![
            spec(&[("dt", "20260722"), ("hh", "10")]),
            spec(&[("dt", "20260723"), ("hh", "10")]),
        ]]
    );

    // A partial specification needs the registry, and reads it once.
    context
        .sql("ALTER TABLE paimon.default.events DROP PARTITION (dt = '20260722')")
        .await
        .unwrap();
    assert_eq!(
        server
            .table_partition_list_name_patterns(DATABASE, TABLE)
            .len(),
        listings + 1
    );
    assert_eq!(
        server
            .table_partition_list_by_names_calls(DATABASE, TABLE)
            .len(),
        1
    );
    assert!(server.table_partition_specs(DATABASE, TABLE).is_empty());
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_a_partition_at_a_custom_location_is_not_taken_from_the_table_directory() {
    let temp_dir = tempfile::tempdir().unwrap();
    let external_dir = tempfile::tempdir().unwrap();
    let schema = format_table_schema(&[("dt", DataType::VarChar(VarCharType::new(255).unwrap()))]);
    let (server, context) = setup_rest_table(&temp_dir, schema).await;
    for dt in ["a", "b"] {
        context
            .sql(&format!(
                "ALTER TABLE paimon.default.events ADD PARTITION (dt = '{dt}')"
            ))
            .await
            .unwrap();
    }
    write_ids(&temp_dir.path().join("dt=a"), &[1]);
    // Another engine registered dt=b somewhere else; the table directory still has a stale copy.
    write_ids(&temp_dir.path().join("dt=b"), &[2]);
    write_ids(external_dir.path(), &[3]);
    server.set_table_partition_options(
        DATABASE,
        TABLE,
        &spec(&[("dt", "b")]),
        HashMap::from([(
            "path".to_string(),
            format!("file://{}", external_dir.path().display()),
        )]),
    );

    // Reading the default directory would return the stale row, so a scan that reaches the
    // partition fails instead. One that does not reach it is unaffected.
    let sql = "SELECT id FROM paimon.default.events WHERE dt = 'b'";
    let error = match context.sql(sql).await {
        Ok(frame) => frame.collect().await.unwrap_err(),
        Err(error) => error,
    }
    .to_string();
    assert!(error.contains("custom location"), "{error}");
    assert_eq!(
        ids(
            &context,
            "SELECT id FROM paimon.default.events WHERE dt = 'a'"
        )
        .await,
        vec![1]
    );

    // Its directory is not under the table, so repair does not read it as missing.
    std::fs::remove_dir_all(temp_dir.path().join("dt=b")).unwrap();
    context
        .sql("MSCK REPAIR TABLE paimon.default.events SYNC PARTITIONS")
        .await
        .unwrap();
    assert!(server
        .table_partition_specs(DATABASE, TABLE)
        .contains(&spec(&[("dt", "b")])));

    // Dropping it unregisters it and deletes nothing, least of all its own data.
    std::fs::create_dir_all(temp_dir.path().join("dt=b")).unwrap();
    context
        .sql("ALTER TABLE paimon.default.events DROP PARTITION (dt = 'b')")
        .await
        .unwrap();
    assert_eq!(
        server.table_partition_specs(DATABASE, TABLE),
        vec![spec(&[("dt", "a")])]
    );
    assert!(external_dir.path().join("part-0.parquet").exists());
    assert_partition_directories(&temp_dir, &[("dt=b", true)]);
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_analyze_measures_registered_partitions_and_replaces_their_statistics() {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = format_table_schema(&[
        ("dt", DataType::VarChar(VarCharType::new(255).unwrap())),
        ("hh", DataType::VarChar(VarCharType::new(255).unwrap())),
    ]);
    let (server, context) = setup_rest_table(&temp_dir, schema).await;
    add_partitions(&context, &[("a", "00"), ("a", "01"), ("b", "00")]).await;
    write_ids_file(&temp_dir.path().join("dt=a/hh=00/part-0.parquet"), &[1, 2]);
    write_ids_file(&temp_dir.path().join("dt=a/hh=00/part-1.parquet"), &[3]);
    write_ids_file(&temp_dir.path().join("dt=a/hh=01/part-0.parquet"), &[4]);

    // NOSCAN measures what a listing gives and leaves the row counts as they were.
    context
        .sql("ANALYZE TABLE paimon.default.events COMPUTE STATISTICS NOSCAN")
        .await
        .unwrap();
    let measured = partition_statistics(&server);
    assert_eq!(counts(&measured["dt=a/hh=00"]), (UNKNOWN, 2));
    assert_eq!(counts(&measured["dt=a/hh=01"]), (UNKNOWN, 1));
    assert_eq!(counts(&measured["dt=b/hh=00"]), (UNKNOWN, 0));
    assert!(measured["dt=a/hh=00"].file_size_in_bytes > 0);
    assert!(measured["dt=a/hh=00"].last_file_creation_time > 0);
    assert_eq!(measured["dt=b/hh=00"].file_size_in_bytes, 0);
    assert_eq!(measured["dt=b/hh=00"].last_file_creation_time, UNKNOWN);

    // A full ANALYZE reads every footer, and an empty partition holds exactly no rows.
    context
        .sql("ANALYZE TABLE paimon.default.events COMPUTE STATISTICS")
        .await
        .unwrap();
    let measured = partition_statistics(&server);
    assert_eq!(counts(&measured["dt=a/hh=00"]), (3, 2));
    assert_eq!(counts(&measured["dt=a/hh=01"]), (1, 1));
    assert_eq!(counts(&measured["dt=b/hh=00"]), (0, 0));

    // A later NOSCAN keeps the known row counts, and measuring again replaces rather than adds.
    context
        .sql("ANALYZE TABLE paimon.default.events COMPUTE STATISTICS NOSCAN")
        .await
        .unwrap();
    let remeasured = partition_statistics(&server);
    assert_eq!(counts(&remeasured["dt=a/hh=00"]), (3, 2));
    assert_eq!(counts(&remeasured["dt=a/hh=01"]), (1, 1));
    assert_eq!(
        remeasured["dt=a/hh=00"].file_size_in_bytes,
        measured["dt=a/hh=00"].file_size_in_bytes
    );

    let calls = server.create_partitions_calls();
    let (_, _, request) = calls.last().unwrap();
    assert!(request.ignore_if_exists);
    assert_eq!(request.replace_statistics, Some(true));
    assert_eq!(server.table_partition_specs(DATABASE, TABLE).len(), 3);
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_analyze_partition_clause_selects_a_leading_run_of_partition_values() {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = format_table_schema(&[
        ("dt", DataType::VarChar(VarCharType::new(255).unwrap())),
        ("hh", DataType::VarChar(VarCharType::new(255).unwrap())),
    ]);
    let (server, context) = setup_rest_table(&temp_dir, schema).await;
    add_partitions(&context, &[("a", "00"), ("a", "01"), ("b", "00")]).await;
    for directory in ["dt=a/hh=00", "dt=a/hh=01", "dt=b/hh=00"] {
        write_ids(&temp_dir.path().join(directory), &[1]);
    }
    let file_counts = |server: &RESTServer| {
        let measured = partition_statistics(server);
        ["dt=a/hh=00", "dt=a/hh=01", "dt=b/hh=00"].map(|name| measured[name].file_count)
    };

    context
        .sql(
            "ANALYZE TABLE paimon.default.events PARTITION (dt = 'a', hh = '00') \
             COMPUTE STATISTICS NOSCAN",
        )
        .await
        .unwrap();
    assert_eq!(file_counts(&server), [1, UNKNOWN, UNKNOWN]);

    // A column named without a value means every value of it.
    context
        .sql("ANALYZE TABLE paimon.default.events PARTITION (dt = 'a', hh) COMPUTE STATISTICS NOSCAN")
        .await
        .unwrap();
    assert_eq!(file_counts(&server), [1, 1, UNKNOWN]);
    context
        .sql("ANALYZE TABLE paimon.default.events PARTITION (dt, hh) COMPUTE STATISTICS NOSCAN")
        .await
        .unwrap();
    assert_eq!(file_counts(&server), [1, 1, 1]);

    for (clause, message) in [
        ("PARTITION (hh = '00')", "leading run"),
        ("PARTITION (id = 1)", "not a partition column"),
        ("PARTITION (dt = 'zzz')", "does not exist"),
    ] {
        let error = context
            .sql(&format!(
                "ANALYZE TABLE paimon.default.events {clause} COMPUTE STATISTICS NOSCAN"
            ))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(message), "{clause}: {error}");
    }
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_analyze_reads_a_partition_value_as_its_column_type() {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = format_table_schema(&[("p", DataType::Int(IntType::new()))]);
    let (server, context) = setup_rest_table(&temp_dir, schema).await;
    context
        .sql("ALTER TABLE paimon.default.events ADD PARTITION (p = 1)")
        .await
        .unwrap();
    write_ids(&temp_dir.path().join("p=1"), &[1]);

    context
        .sql("ANALYZE TABLE paimon.default.events PARTITION (p = '01') COMPUTE STATISTICS NOSCAN")
        .await
        .unwrap();

    assert_eq!(partition_statistics(&server)["p=1"].file_count, 1);
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_analyze_and_scan_count_only_the_files_a_reader_returns() {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = format_table_schema(&[("dt", DataType::VarChar(VarCharType::new(255).unwrap()))]);
    let (server, context) = setup_rest_table(&temp_dir, schema).await;
    context
        .sql("ALTER TABLE paimon.default.events ADD PARTITION (dt = 'a')")
        .await
        .unwrap();
    let partition = temp_dir.path().join("dt=a");
    write_ids_file(&partition.join("part-0.parquet"), &[1]);
    // What committers and tools leave beside the data: staging trees, markers, hidden files.
    write_ids_file(&partition.join("_temporary/0/part-9.parquet"), &[9]);
    write_ids_file(&partition.join("__magic_job-1/tasks/part-8.parquet"), &[8]);
    write_ids_file(&partition.join(".part-7.parquet"), &[7]);
    std::fs::write(partition.join("_SUCCESS"), b"").unwrap();
    std::fs::write(partition.join("notes.txt"), b"not data").unwrap();

    context
        .sql("ANALYZE TABLE paimon.default.events COMPUTE STATISTICS")
        .await
        .unwrap();

    assert_eq!(counts(&partition_statistics(&server)["dt=a"]), (1, 1));
    assert_eq!(
        ids(
            &context,
            "SELECT id FROM paimon.default.events WHERE dt = 'a'"
        )
        .await,
        vec![1]
    );
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_analyze_leaves_a_row_count_unknown_rather_than_short() {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = format_table_schema(&[("dt", DataType::VarChar(VarCharType::new(255).unwrap()))]);
    let (server, context) = setup_rest_table(&temp_dir, schema).await;
    context
        .sql("ALTER TABLE paimon.default.events ADD PARTITION (dt = 'a')")
        .await
        .unwrap();
    write_ids_file(&temp_dir.path().join("dt=a/part-0.parquet"), &[1, 2]);
    std::fs::write(
        temp_dir.path().join("dt=a/part-1.parquet"),
        b"not a parquet footer",
    )
    .unwrap();

    context
        .sql("ANALYZE TABLE paimon.default.events COMPUTE STATISTICS")
        .await
        .unwrap();

    // A sum missing one file, reported as exact, would be worse than no number.
    assert_eq!(counts(&partition_statistics(&server)["dt=a"]), (UNKNOWN, 2));
}

#[cfg(not(windows))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_analyze_refuses_what_it_cannot_measure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = format_table_schema(&[("dt", DataType::VarChar(VarCharType::new(255).unwrap()))]);
    let (server, context) = setup_rest_table(&temp_dir, schema).await;
    for dt in ["a", "b"] {
        context
            .sql(&format!(
                "ALTER TABLE paimon.default.events ADD PARTITION (dt = '{dt}')"
            ))
            .await
            .unwrap();
        write_ids(&temp_dir.path().join(format!("dt={dt}")), &[1]);
    }

    let error = context
        .sql("ANALYZE TABLE paimon.default.events COMPUTE STATISTICS FOR COLUMNS id")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("FOR COLUMNS"), "{error}");

    server.set_table_partition_options(
        DATABASE,
        TABLE,
        &spec(&[("dt", "b")]),
        HashMap::from([("path".to_string(), "file:///elsewhere/b".to_string())]),
    );
    let error = context
        .sql("ANALYZE TABLE paimon.default.events COMPUTE STATISTICS NOSCAN")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("custom location"), "{error}");
    assert!(partition_statistics(&server)
        .values()
        .all(|partition| partition.file_count == UNKNOWN));

    // A non-positive parallelism is read as one rather than failing the statement.
    context
        .sql("SET \"paimon.format-table.statistics.parallelism\" = '0'")
        .await
        .unwrap();
    context
        .sql("ANALYZE TABLE paimon.default.events PARTITION (dt = 'a') COMPUTE STATISTICS")
        .await
        .unwrap();
    assert_eq!(counts(&partition_statistics(&server)["dt=a"]), (1, 1));

    // Without catalog-managed partitions there is no catalog to write the numbers to.
    let plain = Schema::builder()
        .column("dt", DataType::VarChar(VarCharType::new(255).unwrap()))
        .column("id", DataType::BigInt(BigIntType::new()))
        .partition_keys(["dt"])
        .option("type", "format-table")
        .option("file.format", "parquet")
        .build()
        .unwrap();
    server.add_table_with_schema(
        DATABASE,
        "plain",
        plain,
        &format!("file://{}/plain", temp_dir.path().display()),
    );
    server.set_table_external(DATABASE, "plain", false);
    let error = context
        .sql("ANALYZE TABLE paimon.default.plain COMPUTE STATISTICS")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("catalog-managed"), "{error}");
}

/// `SQLContext::sql` futures have to stay `Send` for callers that box them the way
/// `#[async_trait]` does, or spawn them. A stream over borrowed items anywhere below a statement,
/// such as the partition listings ANALYZE runs, takes that away from every statement; this
/// function stops compiling when that happens.
#[allow(dead_code)]
fn sql_future_is_send<'a>(
    context: &'a SQLContext,
    sql: &'a str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        let _ = context.sql(sql).await;
    })
}

const UNKNOWN: i64 = paimon::spec::Partition::UNKNOWN;

/// The partitions the catalog holds, by partition name with keys in name order.
fn partition_statistics(server: &RESTServer) -> HashMap<String, paimon::spec::Partition> {
    server
        .table_partitions(DATABASE, TABLE)
        .into_iter()
        .map(|partition| {
            let mut entries = partition.spec.iter().collect::<Vec<_>>();
            entries.sort();
            let name = entries
                .into_iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join("/");
            (name, partition)
        })
        .collect()
}

fn counts(partition: &paimon::spec::Partition) -> (i64, i64) {
    (partition.record_count, partition.file_count)
}

fn spec(values: &[(&str, &str)]) -> HashMap<String, String> {
    values
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

fn write_ids(directory: &Path, ids: &[i64]) {
    write_ids_file(&directory.join("part-0.parquet"), ids);
}

fn write_ids_file(path: &Path, ids: &[i64]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "id",
        ArrowDataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

async fn ids(context: &SQLContext, sql: &str) -> Vec<i64> {
    let mut ids = Vec::new();
    for batch in context.sql(sql).await.unwrap().collect().await.unwrap() {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        ids.extend(values.iter().flatten());
    }
    ids.sort_unstable();
    ids
}

/// How many listings the table has received from each endpoint: plain, then by filter.
fn listing_counts(server: &RESTServer) -> (usize, usize) {
    (
        server
            .table_partition_list_name_patterns(DATABASE, TABLE)
            .len(),
        server
            .table_partition_list_by_filter_requests(DATABASE, TABLE)
            .len(),
    )
}

/// The name patterns of the listings received since `seen`, whichever endpoint served them.
fn name_patterns_since(server: &RESTServer, seen: (usize, usize)) -> Vec<Option<String>> {
    let mut patterns = server
        .table_partition_list_name_patterns(DATABASE, TABLE)
        .split_off(seen.0);
    patterns.extend(
        server
            .table_partition_list_by_filter_requests(DATABASE, TABLE)
            .into_iter()
            .skip(seen.1)
            .map(|request| request.partition_name_pattern),
    );
    patterns
}

async fn add_partitions(context: &SQLContext, partitions: &[(&str, &str)]) {
    for (dt, hh) in partitions {
        context
            .sql(&format!(
                "ALTER TABLE paimon.default.events \
                 ADD IF NOT EXISTS PARTITION (dt = '{dt}', hh = '{hh}')"
            ))
            .await
            .unwrap();
    }
}

async fn show_partitions(context: &SQLContext) -> Vec<String> {
    let batches = context
        .sql("SHOW PARTITIONS paimon.default.events")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut partitions = Vec::new();
    for batch in batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        partitions.extend((0..batch.num_rows()).map(|index| values.value(index).to_string()));
    }
    partitions
}

async fn assert_partitions(context: &SQLContext, expected: &[&str]) {
    assert_eq!(
        show_partitions(context).await,
        expected
            .iter()
            .map(|partition| (*partition).to_string())
            .collect::<Vec<_>>()
    );
}
