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

//! Scan implementation for Java-compatible `type=format-table` metadata.

use std::collections::{HashMap, HashSet};

use super::format_partition::{
    format_partition_value, is_storage_not_found, parse_format_partition_value,
    FormatTablePartitionPaths,
};
use super::rest_env::LoadedFormatTablePartitionOptions;
use super::{Plan, RESTEnv, ScanTrace, Table};
use crate::spec::stats::BinaryTableStats;
use crate::spec::{
    escape_path_name, extract_datum, unescape_path_name, BinaryRow, BinaryRowBuilder, CoreOptions,
    DataField, DataFileMeta, Datum, PartitionComputer, Predicate, PredicateOperator,
};
use crate::table::partition_filter::PartitionFilter;
use crate::table::source::{DataSplitBuilder, RowRange};

#[derive(Debug, Clone)]
pub(crate) struct FormatTableScan<'a> {
    table: &'a Table,
    partition_filter: Option<PartitionFilter>,
    limit: Option<usize>,
    row_ranges: Option<Vec<RowRange>>,
}

impl<'a> FormatTableScan<'a> {
    pub(crate) fn new(
        table: &'a Table,
        partition_filter: Option<PartitionFilter>,
        limit: Option<usize>,
        row_ranges: Option<Vec<RowRange>>,
    ) -> Self {
        Self {
            table,
            partition_filter,
            limit,
            row_ranges,
        }
    }

    pub(crate) fn with_row_ranges(mut self, ranges: Vec<RowRange>) -> Self {
        self.row_ranges = Some(ranges);
        self
    }

    pub(crate) async fn plan(&self) -> crate::Result<Plan> {
        self.ensure_query_auth_allowed()?;
        self.plan_inner(None).await
    }

    pub(crate) async fn plan_with_trace(&self) -> crate::Result<(Plan, ScanTrace)> {
        self.ensure_query_auth_allowed()?;
        let mut trace = ScanTrace::default();
        let plan = self.plan_inner(Some(&mut trace)).await?;
        trace.planned_data_file_bytes = plan.planned_data_file_bytes();
        Ok((plan, trace))
    }

    fn ensure_query_auth_allowed(&self) -> crate::Result<()> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()
    }

    async fn plan_inner(&self, trace: Option<&mut ScanTrace>) -> crate::Result<Plan> {
        if self.row_ranges.is_some() {
            return Err(crate::Error::Unsupported {
                message: "Row ranges are not supported for format tables".to_string(),
            });
        }
        let core_options = CoreOptions::new(self.table.schema().options());
        let managed_options = self
            .table
            .rest_env()
            .and_then(RESTEnv::catalog_managed_partition_options);
        let file_format = managed_options
            .map(|options| options.file_format.clone())
            .unwrap_or_else(|| core_options.file_format());
        let format_extension = supported_format_table_extension(&file_format)?;
        let schema_id = self.table.schema().id();
        let table_path = managed_options
            .map(|options| options.table_path.as_str())
            .or_else(|| core_options.path())
            .unwrap_or_else(|| self.table.location())
            .trim_end_matches('/')
            .to_string();

        let partition_fields = self.table.schema().partition_fields();
        let mut splits = Vec::new();
        for scan_root in self.scan_roots(&core_options, &table_path).await? {
            let statuses = self
                .list_status_recursive_if_exists(&scan_root.path)
                .await?;
            for status in statuses {
                if let Some(split) = self
                    .status_to_split(
                        status,
                        &table_path,
                        format_extension,
                        schema_id,
                        &partition_fields,
                        scan_root.partition.clone(),
                    )
                    .await?
                {
                    splits.push(split);
                }
            }
        }

        splits.sort_by(|left, right| {
            left.bucket_path().cmp(right.bucket_path()).then_with(|| {
                left.data_files()[0]
                    .file_name
                    .cmp(&right.data_files()[0].file_name)
            })
        });
        splits = self.apply_limit_pushdown(splits);

        if let Some(trace) = trace {
            trace.record_final_plan(splits.len(), splits.len(), splits.len());
        }
        Ok(Plan::new(splits))
    }

    async fn scan_roots(
        &self,
        core_options: &CoreOptions<'_>,
        table_path: &str,
    ) -> crate::Result<Vec<ScanRoot>> {
        let partition_keys = self.table.schema().partition_keys();
        let partition_fields = self.table.schema().partition_fields();
        if partition_keys.is_empty() {
            return Ok(vec![ScanRoot {
                path: table_path.to_string(),
                partition: BinaryRow::new(0),
            }]);
        }
        if let Some(rest_env) = self.table.rest_env() {
            if let Some(managed_options) = rest_env.catalog_managed_partition_options() {
                return self
                    .catalog_managed_scan_roots(
                        rest_env,
                        table_path,
                        partition_keys,
                        &partition_fields,
                        managed_options,
                    )
                    .await;
            }
        }

        let Some(PartitionFilter::PartitionSet { partitions, .. }) = &self.partition_filter else {
            if let Some(PartitionFilter::Predicate(predicate)) = &self.partition_filter {
                if let Some(path) = leading_equality_partition_path(
                    table_path,
                    partition_keys,
                    &partition_fields,
                    predicate,
                    core_options.partition_default_name(),
                    core_options.legacy_partition_name(),
                    core_options.format_table_partition_only_value_in_path(),
                ) {
                    return Ok(vec![ScanRoot {
                        path,
                        partition: BinaryRow::new(0),
                    }]);
                }
            }
            return Ok(vec![ScanRoot {
                path: table_path.to_string(),
                partition: BinaryRow::new(0),
            }]);
        };

        let partition_computer = PartitionComputer::new(
            partition_keys,
            self.table.schema().fields(),
            core_options.partition_default_name(),
            core_options.legacy_partition_name(),
        )?;
        let only_value_in_path = core_options.format_table_partition_only_value_in_path();
        let mut roots = Vec::with_capacity(partitions.len());
        for partition in partitions {
            let row = BinaryRow::from_serialized_bytes(partition)?;
            let partition_path = if only_value_in_path {
                partition_path_from_row(
                    &row,
                    &partition_fields,
                    core_options.partition_default_name(),
                    core_options.legacy_partition_name(),
                    true,
                )?
            } else {
                partition_computer.generate_partition_path(&row)?
            };
            roots.push(ScanRoot {
                path: join_path(table_path, &partition_path),
                partition: row,
            });
        }
        roots.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(roots)
    }

    async fn catalog_managed_scan_roots(
        &self,
        rest_env: &RESTEnv,
        table_path: &str,
        partition_keys: &[String],
        partition_fields: &[DataField],
        managed_options: &LoadedFormatTablePartitionOptions,
    ) -> crate::Result<Vec<ScanRoot>> {
        let only_value_in_path = managed_options.only_value_in_path;
        let partition_paths =
            FormatTablePartitionPaths::new(partition_keys.iter().cloned(), only_value_in_path);
        let core_options = CoreOptions::new(self.table.schema().options());
        let default_partition_name = core_options.partition_default_name();
        // Ask the catalog only for the partitions the filter can reach. Downloading every
        // registration of a table with many partitions is what dominates planning time,
        // and the local match below still decides what is actually scanned.
        let pattern = match &self.partition_filter {
            Some(filter) => {
                let leading_values = leading_equality_partition_values(
                    filter,
                    partition_fields,
                    default_partition_name,
                    core_options.legacy_partition_name(),
                )?;
                partition_paths.name_prefix_pattern(&leading_values)
            }
            None => None,
        };
        let partitions = rest_env
            .api()
            .list_partitions_by_name_pattern(rest_env.identifier(), pattern.as_deref())
            .await?;
        let mut seen_paths = HashSet::with_capacity(partitions.len());
        let mut roots = Vec::with_capacity(partitions.len());
        for partition in partitions {
            let partition_path = partition_paths
                .relative_path(&partition.spec)
                .map_err(|error| self.invalid_catalog_partition_metadata(error))?;
            if !seen_paths.insert(partition_path.clone()) {
                continue;
            }
            let path = join_path(table_path, &partition_path);
            let partition = partition_row_from_catalog_spec(
                &partition.spec,
                partition_fields,
                partition_keys,
                default_partition_name,
            )
            .map_err(|error| self.invalid_catalog_partition_metadata(error))?;
            if self.partition_matches(&partition)? {
                roots.push(ScanRoot { path, partition });
            }
        }
        roots.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(roots)
    }

    fn invalid_catalog_partition_metadata(&self, source: crate::Error) -> crate::Error {
        crate::Error::DataInvalid {
            message: format!(
                "Catalog returned invalid partition metadata for Format Table {}",
                self.table.identifier().full_name()
            ),
            source: Some(Box::new(source)),
        }
    }

    async fn list_status_recursive_if_exists(
        &self,
        path: &str,
    ) -> crate::Result<Vec<crate::io::FileStatus>> {
        match self.table.file_io().list_status_recursive(path).await {
            Ok(statuses) => Ok(statuses),
            Err(err) if is_storage_not_found(&err) => Ok(Vec::new()),
            Err(err) => Err(err),
        }
    }

    async fn status_to_split(
        &self,
        status: crate::io::FileStatus,
        table_path: &str,
        format_extension: &str,
        schema_id: i64,
        partition_fields: &[DataField],
        known_partition: BinaryRow,
    ) -> crate::Result<Option<crate::DataSplit>> {
        let Some((parent, file_name)) = split_parent_and_file(&status.path) else {
            return Ok(None);
        };
        let parent = parent.to_string();
        let file_name = file_name.to_string();
        if !is_format_table_data_file_name(&file_name) {
            return Ok(None);
        }
        if !file_name.to_ascii_lowercase().ends_with(format_extension) {
            return Ok(None);
        }
        let status = if status.size == 0 {
            self.table.file_io().get_status(&status.path).await?
        } else {
            status
        };
        let file_size = i64::try_from(status.size).map_err(|_| crate::Error::DataInvalid {
            message: format!(
                "Format table file '{}' is too large to fit in i64 metadata",
                status.path
            ),
            source: None,
        })?;
        let data_file = data_file_meta(file_name, file_size, schema_id);
        let partition = if partition_fields.is_empty() {
            BinaryRow::new(0)
        } else if known_partition.arity() == partition_fields.len() as i32
            && !known_partition.is_empty()
        {
            known_partition
        } else {
            let core_options = CoreOptions::new(self.table.schema().options());
            let Some(partition) = partition_row_from_path(
                table_path,
                &parent,
                partition_fields,
                self.table.schema().partition_keys(),
                core_options.partition_default_name(),
                core_options.format_table_partition_only_value_in_path(),
            )?
            else {
                return Ok(None);
            };
            if !self.partition_matches(&partition)? {
                return Ok(None);
            }
            partition
        };

        Ok(Some(
            DataSplitBuilder::new()
                .with_snapshot(0)
                .with_partition(partition)
                .with_bucket(0)
                .with_bucket_path(parent)
                .with_total_buckets(1)
                .with_data_files(vec![data_file])
                .with_raw_convertible(true)
                .build()?,
        ))
    }

    fn partition_matches(&self, partition: &BinaryRow) -> crate::Result<bool> {
        match &self.partition_filter {
            Some(filter) => filter.matches_entry(&partition.to_serialized_bytes()),
            None => Ok(true),
        }
    }

    /// A limit of zero needs no split at all. A positive one keeps every split: a format
    /// table has no row counts, so the number of files says nothing about the number of
    /// rows, and dropping files would answer the query from a guess.
    ///
    /// Mirrors Java `FormatTableScan.FormatTableScanPlan.splits`.
    pub(crate) fn apply_limit_pushdown(
        &self,
        splits: Vec<crate::DataSplit>,
    ) -> Vec<crate::DataSplit> {
        match self.limit {
            Some(0) => Vec::new(),
            _ => splits,
        }
    }
}

#[derive(Debug, Clone)]
struct ScanRoot {
    path: String,
    partition: BinaryRow,
}

fn is_format_table_data_file_name(file_name: &str) -> bool {
    !file_name.is_empty() && !file_name.starts_with('.') && !file_name.starts_with('_')
}

fn split_parent_and_file(path: &str) -> Option<(&str, &str)> {
    let trimmed = path.trim_end_matches('/');
    let slash = trimmed.rfind('/')?;
    Some((&trimmed[..slash], &trimmed[slash + 1..]))
}

fn join_path(parent: &str, child: &str) -> String {
    let parent = parent.trim_end_matches('/');
    let child = child.trim_start_matches('/').trim_end_matches('/');
    if child.is_empty() {
        parent.to_string()
    } else {
        format!("{parent}/{child}")
    }
}

fn leading_equality_partition_path(
    table_path: &str,
    partition_keys: &[String],
    partition_fields: &[DataField],
    predicate: &Predicate,
    default_partition_name: &str,
    legacy_partition_name: bool,
    only_value_in_path: bool,
) -> Option<String> {
    let values = leading_equality_values_from_predicate(
        predicate,
        partition_fields,
        default_partition_name,
        legacy_partition_name,
    );
    if values.is_empty() {
        return None;
    }
    let segments = partition_keys
        .iter()
        .zip(&values)
        .map(|(key, value)| {
            if only_value_in_path {
                escape_path_name(value)
            } else {
                format!("{}={}", escape_path_name(key), escape_path_name(value))
            }
        })
        .collect::<Vec<_>>();
    Some(join_path(table_path, &segments.join("/")))
}

/// The leading run of partition values this filter pins to a single value, in
/// partition-key order, formatted the way the catalog and the partition path spell them.
///
/// Only a leading run is useful: a partition path prefix and the partition-name pattern a
/// catalog prunes on can express nothing else. A null value has no such spelling, so the
/// run stops there.
fn leading_equality_partition_values(
    filter: &PartitionFilter,
    partition_fields: &[DataField],
    default_partition_name: &str,
    legacy_partition_name: bool,
) -> crate::Result<Vec<String>> {
    match filter {
        PartitionFilter::Predicate(predicate) => Ok(leading_equality_values_from_predicate(
            predicate,
            partition_fields,
            default_partition_name,
            legacy_partition_name,
        )),
        // An enumerated partition set still pins a prefix whenever its rows agree on one,
        // which is what `dt = 'a' AND hh IN ('10', '11')` collapses to.
        PartitionFilter::PartitionSet { partitions, .. } => {
            let mut common: Option<Vec<String>> = None;
            for serialized in partitions {
                let row = BinaryRow::from_serialized_bytes(serialized)?;
                let mut values = Vec::with_capacity(partition_fields.len());
                for (index, field) in partition_fields.iter().enumerate() {
                    let Some(datum) = extract_datum(&row, index, field.data_type())? else {
                        break;
                    };
                    let Some(value) = format_partition_value(
                        &datum,
                        field.data_type(),
                        default_partition_name,
                        legacy_partition_name,
                    ) else {
                        break;
                    };
                    values.push(value);
                }
                common = Some(match common {
                    None => values,
                    Some(common) => common
                        .into_iter()
                        .zip(values)
                        .take_while(|(left, right)| left == right)
                        .map(|(left, _)| left)
                        .collect(),
                });
                if common.as_ref().is_some_and(Vec::is_empty) {
                    break;
                }
            }
            Ok(common.unwrap_or_default())
        }
    }
}

/// Mirrors Java `FormatTableScan.extractLeadingEqualityPartitionSpecWhenOnlyAnd`.
fn leading_equality_values_from_predicate(
    predicate: &Predicate,
    partition_fields: &[DataField],
    default_partition_name: &str,
    legacy_partition_name: bool,
) -> Vec<String> {
    let mut pinned: Vec<Option<&Datum>> = vec![None; partition_fields.len()];
    let predicates = predicate.clone().split_and();
    for predicate in &predicates {
        let Predicate::Leaf {
            index,
            op: PredicateOperator::Eq,
            literals,
            ..
        } = predicate
        else {
            continue;
        };
        if *index < pinned.len() {
            pinned[*index] = literals.first();
        }
    }

    let mut values = Vec::new();
    for (field, datum) in partition_fields.iter().zip(pinned) {
        let Some(datum) = datum else {
            break;
        };
        let Some(value) = format_partition_value(
            datum,
            field.data_type(),
            default_partition_name,
            legacy_partition_name,
        ) else {
            break;
        };
        values.push(value);
    }
    values
}

fn partition_path_from_row(
    row: &BinaryRow,
    partition_fields: &[DataField],
    default_partition_name: &str,
    legacy_partition_name: bool,
    only_value_in_path: bool,
) -> crate::Result<String> {
    let mut segments = Vec::with_capacity(partition_fields.len());
    for (idx, field) in partition_fields.iter().enumerate() {
        let value = match extract_datum(row, idx, field.data_type())? {
            None => default_partition_name.to_string(),
            Some(datum) => format_partition_value(
                &datum,
                field.data_type(),
                default_partition_name,
                legacy_partition_name,
            )
            .ok_or_else(|| crate::Error::Unsupported {
                message: format!(
                    "Format table partition path generation does not support type '{:?}'",
                    field.data_type()
                ),
            })?,
        };
        if only_value_in_path {
            segments.push(escape_path_name(&value));
        } else {
            segments.push(format!(
                "{}={}",
                escape_path_name(field.name()),
                escape_path_name(&value)
            ));
        }
    }
    Ok(segments.join("/"))
}

fn partition_row_from_path(
    table_path: &str,
    file_parent: &str,
    partition_fields: &[DataField],
    partition_keys: &[String],
    default_partition_name: &str,
    only_value_in_path: bool,
) -> crate::Result<Option<BinaryRow>> {
    let relative = match file_parent
        .trim_end_matches('/')
        .strip_prefix(table_path.trim_end_matches('/'))
    {
        Some(path) => path.trim_start_matches('/'),
        None => return Ok(None),
    };
    if relative.is_empty() {
        return Ok(None);
    }

    let mut values = Vec::with_capacity(partition_keys.len());
    if only_value_in_path {
        for segment in relative
            .split('/')
            .filter(|segment| !segment.is_empty())
            .take(partition_keys.len())
        {
            let Some(value) = unescape_path_name(segment) else {
                return Ok(None);
            };
            values.push(value);
        }
        if values.len() != partition_keys.len() {
            return Ok(None);
        }
    } else {
        for key in partition_keys {
            let Some(value) = relative
                .split('/')
                .find_map(|segment| partition_segment_value(segment, key))
            else {
                return Ok(None);
            };
            values.push(value);
        }
    }

    let mut builder = BinaryRowBuilder::new(partition_fields.len() as i32);
    for (idx, value) in values.iter().enumerate() {
        if value == default_partition_name {
            builder.set_null_at(idx);
            continue;
        }
        let Some(datum) = parse_format_partition_value(value, partition_fields[idx].data_type())
        else {
            return Ok(None);
        };
        builder.write_datum(idx, &datum, partition_fields[idx].data_type());
    }
    Ok(Some(builder.build()))
}

fn partition_row_from_catalog_spec(
    spec: &HashMap<String, String>,
    partition_fields: &[DataField],
    partition_keys: &[String],
    default_partition_name: &str,
) -> crate::Result<BinaryRow> {
    let mut builder = BinaryRowBuilder::new(partition_fields.len() as i32);
    for (index, (key, field)) in partition_keys.iter().zip(partition_fields).enumerate() {
        let value = spec.get(key).ok_or_else(|| crate::Error::DataInvalid {
            message: format!("Catalog partition is missing column '{key}'"),
            source: None,
        })?;
        if value == default_partition_name {
            builder.set_null_at(index);
            continue;
        }
        let datum = parse_format_partition_value(value, field.data_type()).ok_or_else(|| {
            crate::Error::DataInvalid {
                message: format!(
                    "Invalid catalog partition value {value:?} for column '{key}' with type {:?}",
                    field.data_type()
                ),
                source: None,
            }
        })?;
        builder.write_datum(index, &datum, field.data_type());
    }
    Ok(builder.build())
}

fn partition_segment_value(segment: &str, key: &str) -> Option<String> {
    let (segment_key, segment_value) = segment.split_once('=')?;
    if unescape_path_name(segment_key)? == key {
        unescape_path_name(segment_value)
    } else {
        None
    }
}

fn supported_format_table_formats() -> Vec<&'static str> {
    vec![
        "parquet",
        "orc",
        "avro",
        "row",
        "mosaic",
        #[cfg(feature = "vortex")]
        "vortex",
    ]
}

fn supported_format_table_extension(format: &str) -> crate::Result<&'static str> {
    match format.to_ascii_lowercase().as_str() {
        "parquet" => Ok(".parquet"),
        "orc" => Ok(".orc"),
        "avro" => Ok(".avro"),
        "row" => Ok(".row"),
        "mosaic" => Ok(".mosaic"),
        #[cfg(feature = "vortex")]
        "vortex" => Ok(".vortex"),
        other => Err(crate::Error::Unsupported {
            message: format!(
                "Format table file.format '{other}' is not supported by the Rust reader yet, \
                 expected one of: {}",
                supported_format_table_formats().join(", ")
            ),
        }),
    }
}

fn data_file_meta(file_name: String, file_size: i64, schema_id: i64) -> DataFileMeta {
    DataFileMeta {
        file_name,
        file_size,
        row_count: DataFileMeta::ROW_COUNT_UNKNOWN,
        min_key: Vec::new(),
        max_key: Vec::new(),
        key_stats: BinaryTableStats::empty(),
        value_stats: BinaryTableStats::empty(),
        min_sequence_number: 0,
        max_sequence_number: 0,
        schema_id,
        level: 0,
        extra_files: Vec::new(),
        creation_time: None,
        delete_row_count: Some(0),
        embedded_index: None,
        file_source: None,
        value_stats_cols: None,
        external_path: None,
        first_row_id: None,
        write_cols: None,
        column_max_sequence_numbers: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unsupported_format_lists_the_supported_ones() {
        let error = supported_format_table_extension("csv").unwrap_err();
        let crate::Error::Unsupported { message } = error else {
            panic!("expected Unsupported, got {error:?}");
        };
        assert!(message.contains("'csv'"), "{message}");
        for format in supported_format_table_formats() {
            assert!(message.contains(format), "{format} missing from {message}");
        }
    }

    #[test]
    fn test_supported_formats_are_accepted_case_insensitively() {
        for format in supported_format_table_formats() {
            let expected = format!(".{format}");
            assert_eq!(supported_format_table_extension(format).unwrap(), expected);
            assert_eq!(
                supported_format_table_extension(&format.to_ascii_uppercase()).unwrap(),
                expected
            );
        }
    }
}
