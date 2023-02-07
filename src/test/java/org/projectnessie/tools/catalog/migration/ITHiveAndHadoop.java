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
package org.projectnessie.tools.catalog.migration;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.projectnessie.tools.catalog.migration.CatalogMigrationCLI.DRY_RUN_FILE;
import static org.projectnessie.tools.catalog.migration.CatalogMigrationCLI.FAILED_IDENTIFIERS_FILE;
import static org.projectnessie.tools.catalog.migration.CatalogMigrationCLI.FAILED_TO_DELETE_AT_SOURCE_FILE;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveMetastoreTest;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class ITHiveAndHadoop extends HiveMetastoreTest {

  private static String warehousePath1;

  private static @TempDir File warehouse2;
  private static String warehousePath2;

  private static Catalog catalog1;

  private static Catalog catalog2;

  private static final Schema schema =
      new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());

  @BeforeAll
  protected static void setup() throws Exception {
    startMetastore();
    warehousePath1 = catalog.getConf().get("hive.metastore.warehouse.dir");
    warehousePath2 = String.format("file://%s", warehouse2.getAbsolutePath());

    catalog1 = createHadoopCatalog(warehousePath2, "catalog1");
    ((SupportsNamespaces) catalog1).createNamespace(Namespace.of("foo"), Collections.emptyMap());
    ((SupportsNamespaces) catalog1).createNamespace(Namespace.of("bar"), Collections.emptyMap());

    // assign to hive catalog from the parent class
    catalog2 = catalog;
    ((SupportsNamespaces) catalog2).createNamespace(Namespace.of("foo"), Collections.emptyMap());
    ((SupportsNamespaces) catalog2).createNamespace(Namespace.of("bar"), Collections.emptyMap());
  }

  @AfterAll
  protected static void tearDown() throws Exception {
    stopMetastore();
  }

  @BeforeEach
  protected void beforeEach() {
    // two tables in 'foo' namespace
    catalog1.createTable(TableIdentifier.of(Namespace.of("foo"), "tbl1"), schema);
    catalog1.createTable(TableIdentifier.of(Namespace.of("foo"), "tbl2"), schema);
    // two tables in 'bar' namespace
    catalog1.createTable(TableIdentifier.of(Namespace.of("bar"), "tbl3"), schema);
    catalog1.createTable(TableIdentifier.of(Namespace.of("bar"), "tbl4"), schema);

    // one table in catalog2
    catalog2.createTable(TableIdentifier.of(Namespace.of("bar"), "tbl5"), schema);
  }

  @AfterEach
  protected void afterEach() {
    Arrays.asList(Namespace.of("foo"), Namespace.of("bar"))
        .forEach(
            namespace -> {
              catalog1.listTables(namespace).forEach(catalog1::dropTable);
              catalog2.listTables(namespace).forEach(catalog2::dropTable);
            });
    TestUtil.deleteFileIfExists(FAILED_IDENTIFIERS_FILE);
    TestUtil.deleteFileIfExists(FAILED_TO_DELETE_AT_SOURCE_FILE);
    TestUtil.deleteFileIfExists(DRY_RUN_FILE);
  }

  private static Catalog createHadoopCatalog(String warehousePath, String name) {
    Map<String, String> properties = new HashMap<>();
    properties.put("warehouse", warehousePath);
    properties.put("type", "hadoop");
    return CatalogUtil.loadCatalog(
        HadoopCatalog.class.getName(), name, properties, new Configuration());
  }

  @Test
  @Order(0)
  public void testRegister() throws Exception {
    TestUtil.respondAsContinue();
    RunCLI run =
        RunCLI.run(
            "--source-catalog-type",
            "HADOOP",
            "--source-catalog-properties",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "--target-catalog-type",
            "HIVE",
            "--target-catalog-properties",
            "warehouse=" + warehousePath1 + ",uri=" + catalog.getConf().get("hive.metastore.uris"));

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut())
        .contains(
            "User has not specified the table identifiers. "
                + "Selecting all the tables from all the namespaces from the source catalog.");
    Assertions.assertThat(run.getOut()).contains("Identified 4 tables for registration.");
    Assertions.assertThat(run.getOut())
        .contains(
            "Summary: \n- Successfully registered 4 tables from HADOOP catalog to"
                + " HIVE catalog.");
    Assertions.assertThat(run.getOut())
        .contains("Details: \n" + "- Successfully registered these tables:\n");
    Assertions.assertThat(catalog2.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl3"),
            TableIdentifier.parse("bar.tbl4"),
            TableIdentifier.parse("bar.tbl5"));
  }

  @Test
  @Order(1)
  public void testMigrate() throws Exception {
    TestUtil.respondAsContinue();
    RunCLI run =
        RunCLI.run(
            "--source-catalog-type",
            "HIVE",
            "--source-catalog-properties",
            "warehouse=" + warehousePath1 + ",uri=" + catalog.getConf().get("hive.metastore.uris"),
            "--target-catalog-type",
            "HADOOP",
            "--target-catalog-properties",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "--delete-source-tables");

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut())
        .contains(
            "User has not specified the table identifiers. "
                + "Selecting all the tables from all the namespaces from the source catalog.");
    Assertions.assertThat(run.getOut()).contains("Identified 1 tables for migration.");
    Assertions.assertThat(run.getOut())
        .contains(
            "Summary: \n- Successfully migrated 1 tables from HIVE catalog to"
                + " HADOOP catalog.");
    Assertions.assertThat(run.getOut())
        .contains("Details: \n" + "- Successfully migrated these tables:\n");
    // migrated table should be present in the target catalog
    Assertions.assertThat(catalog1.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl5"), // tbl5 is the migrated table
            TableIdentifier.parse("bar.tbl4"),
            TableIdentifier.parse("bar.tbl3"));

    // migrated table should not be there in the source catalog
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar"))).isEmpty();
  }
}
