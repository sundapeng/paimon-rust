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

use std::collections::{BTreeMap, HashMap, HashSet};

use paimon::catalog::{Catalog, Identifier};
use paimon::spec::CoreOptions;
use paimon::table::{FormatTablePartitionPaths, Table};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepairMode {
    Add,
    Drop,
    Sync,
}

pub(crate) async fn repair(
    catalog: &dyn Catalog,
    identifier: &Identifier,
    table: &Table,
    mode: RepairMode,
) -> paimon::Result<()> {
    let core_options = CoreOptions::new(table.schema().options());
    let partition_paths = FormatTablePartitionPaths::new(
        table.schema().partition_keys().iter().cloned(),
        core_options.format_table_partition_only_value_in_path(),
    );
    let table_path = table.location();

    if matches!(mode, RepairMode::Drop | RepairMode::Sync) {
        // Discovery treats a missing root as empty. Destructive repair must fail
        // instead, or it could unregister every catalog partition.
        table.file_io().list_status(table_path).await?;
    }

    // Load both views before changing catalog metadata so a listing failure leaves
    // metadata unchanged. Discovery preserves raw directory values such as month=01.
    let discovered_specs = partition_paths
        .discover(
            table.file_io(),
            table_path,
            core_options.partition_default_name(),
        )
        .await?;
    let registered_partitions = catalog.list_partitions(identifier).await?;
    // A partition registered at a location of its own does not live under the table directory,
    // so discovery never finds it there. Repair leaves it registered rather than reading that as
    // a directory gone missing.
    let custom_located = registered_partitions
        .iter()
        .filter(|partition| {
            partition
                .options
                .as_ref()
                .is_some_and(|options| options.contains_key("path"))
        })
        .map(|partition| partition_paths.partition_name(&partition.spec))
        .collect::<paimon::Result<HashSet<_>>>()?;

    let discovered_by_name = index_specs_by_name(&partition_paths, discovered_specs)?;
    let registered_by_name = index_specs_by_name(
        &partition_paths,
        registered_partitions
            .into_iter()
            .map(|partition| partition.spec)
            .collect(),
    )?;

    let to_register = if matches!(mode, RepairMode::Add | RepairMode::Sync) {
        discovered_by_name
            .iter()
            .filter(|(name, _)| !registered_by_name.contains_key(*name))
            .map(|(_, spec)| spec.clone())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let to_unregister = if matches!(mode, RepairMode::Drop | RepairMode::Sync) {
        registered_by_name
            .iter()
            .filter(|(name, _)| {
                !discovered_by_name.contains_key(*name) && !custom_located.contains(*name)
            })
            .map(|(_, spec)| spec.clone())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    if !to_register.is_empty() {
        catalog
            .create_partitions(identifier, to_register, true)
            .await?;
    }
    if !to_unregister.is_empty() {
        catalog.drop_partitions(identifier, to_unregister).await?;
    }
    Ok(())
}

fn index_specs_by_name(
    partition_paths: &FormatTablePartitionPaths,
    specs: Vec<HashMap<String, String>>,
) -> paimon::Result<BTreeMap<String, HashMap<String, String>>> {
    specs
        .into_iter()
        .map(|spec| Ok((partition_paths.partition_name(&spec)?, spec)))
        .collect()
}
