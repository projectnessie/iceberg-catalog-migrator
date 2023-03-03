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
import picocli.CommandLine;

public class TargetCatalogOptions {

  @CommandLine.Option(
      names = "--target-catalog-type",
      required = true,
      description =
          "target catalog type. "
              + "Can be one of these [CUSTOM, DYNAMODB, ECS, GLUE, HADOOP, HIVE, JDBC, NESSIE, REST]")
  private CatalogUtil.CatalogType type;

  @CommandLine.Option(
      names = "--target-catalog-properties",
      required = true,
      split = ",",
      description = "target catalog properties (like uri, warehouse, etc)")
  private Map<String, String> properties;

  @CommandLine.Option(
      names = "--target-catalog-hadoop-conf",
      split = ",",
      description =
          "optional target catalog Hadoop configurations (like fs.s3a.secret.key, fs.s3a.access.key) required when "
              + "using an Iceberg FileIO.")
  private Map<String, String> hadoopConf = new HashMap<>();

  @CommandLine.Option(
      names = {"--target-custom-catalog-impl"},
      description =
          "optional fully qualified class name of the custom catalog implementation of the target catalog. Required "
              + "when the catalog type is CUSTOM.")
  private String customCatalogImpl;

  protected Catalog build() {
    return CatalogUtil.buildCatalog(properties, type, customCatalogImpl, hadoopConf);
  }
}
