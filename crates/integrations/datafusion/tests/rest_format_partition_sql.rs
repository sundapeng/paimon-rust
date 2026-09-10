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
        let seen = server
            .table_partition_list_name_patterns(DATABASE, TABLE)
            .len();
        context
            .sql(&format!(
                "SELECT * FROM paimon.default.events WHERE {predicate}"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let patterns = server.table_partition_list_name_patterns(DATABASE, TABLE);
        let pushed = &patterns[seen..];
        assert!(!pushed.is_empty(), "{predicate} listed no partitions");
        assert!(
            pushed.iter().all(|pattern| pattern.as_deref() == expected),
            "{predicate} pushed {pushed:?}, expected {expected:?}"
        );
    }
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

fn write_ids(directory: &Path, ids: &[i64]) {
    std::fs::create_dir_all(directory).unwrap();
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
    let file = std::fs::File::create(directory.join("part-0.parquet")).unwrap();
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
