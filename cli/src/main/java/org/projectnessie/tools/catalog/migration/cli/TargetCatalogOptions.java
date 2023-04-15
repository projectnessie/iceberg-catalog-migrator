/*
 * Copyright (C) 2023 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.tools.catalog.migration.cli;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.projectnessie.tools.catalog.migration.api.CatalogMigrationUtil;
import picocli.CommandLine;

public class TargetCatalogOptions {

  @CommandLine.Option(
      names = "--target-catalog-type",
      required = true,
      description = {
        "Target catalog type. Can be one of these [CUSTOM, DYNAMODB, ECS, GLUE, HADOOP, HIVE, JDBC, "
            + "NESSIE, REST].",
        "Example: --target-catalog-type GLUE",
        "         --target-catalog-type NESSIE"
      })
  protected CatalogMigrationUtil.CatalogType type;

  @CommandLine.Option(
      names = "--target-catalog-properties",
      required = true,
      split = ",",
      description = {
        "Iceberg catalog properties for target catalog (like uri, warehouse, etc).",
        "Example: --target-catalog-properties uri=http://localhost:19120/api/v1,ref=main,warehouse=/tmp/warehouseNessie"
      })
  private Map<String, String> properties;

  @CommandLine.Option(
      names = "--target-catalog-hadoop-conf",
      split = ",",
      description = {
        "Optional target catalog Hadoop configurations required by the Iceberg catalog.",
        "Example: --target-catalog-hadoop-conf key1=value1,key2=value2"
      })
  private final Map<String, String> hadoopConf = new HashMap<>();

  @CommandLine.Option(
      names = {"--target-custom-catalog-impl"},
      description = {
        "Optional fully qualified class name of the custom catalog implementation of the target catalog. Required "
            + "when the catalog type is CUSTOM.",
        "Example: --target-custom-catalog-impl org.apache.iceberg.AwesomeCatalog"
      })
  private String customCatalogImpl;

  Catalog build() {
    return CatalogMigrationUtil.buildCatalog(
        properties, type, "TARGET_CATALOG_" + type.name(), customCatalogImpl, hadoopConf);
  }
}
