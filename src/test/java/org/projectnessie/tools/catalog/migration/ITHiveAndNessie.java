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

import java.io.ByteArrayInputStream;
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
import org.apache.iceberg.hive.HiveMetastoreTest;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;

public class ITHiveAndNessie extends HiveMetastoreTest {

  private static String warehousePath1;

  private static @TempDir File warehouse2;
  private static String warehousePath2;

  private static Catalog catalog1;

  private static Catalog catalog2;

  private static final Schema schema =
      new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());

  private static final String IMAGE = "projectnessie/nessie:0.47.1";
  private static final int NESSIE_PORT = 19121;

  private static String nessieUri;

  private static GenericContainer<?> container;

  @BeforeAll
  protected static void setup() {
    try {
      startMetastore();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    warehousePath1 = catalog.getConf().get("hive.metastore.warehouse.dir");
    warehousePath2 = String.format("file://%s", warehouse2.getAbsolutePath());

    container =
        new GenericContainer(IMAGE)
            .withExposedPorts(NESSIE_PORT)
            .withEnv("QUARKUS_HTTP_PORT", String.valueOf(NESSIE_PORT));

    container.start();

    nessieUri =
        String.format(
            "http://%s:%s/api/v1", container.getHost(), container.getMappedPort(NESSIE_PORT));

    // assign to hive catalog from the parent class
    catalog1 = catalog;
    ((SupportsNamespaces) catalog1).createNamespace(Namespace.of("foo"), Collections.emptyMap());
    ((SupportsNamespaces) catalog1).createNamespace(Namespace.of("bar"), Collections.emptyMap());

    catalog2 = createNessieCatalog(warehousePath2, nessieUri);
    ((SupportsNamespaces) catalog2).createNamespace(Namespace.of("foo"), Collections.emptyMap());
    ((SupportsNamespaces) catalog2).createNamespace(Namespace.of("bar"), Collections.emptyMap());
  }

  @AfterAll
  protected static void tearDown() {
    try {
      stopMetastore();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    container.stop();
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
  }

  private static Catalog createNessieCatalog(String warehousePath, String uri) {
    Map<String, String> properties = new HashMap<>();
    properties.put("warehouse", warehousePath);
    properties.put("ref", "main");
    properties.put("uri", uri);
    return CatalogUtil.loadCatalog(
        NessieCatalog.class.getName(), "nessie", properties, new Configuration());
  }

  @Test
  @Order(0)
  public void testRegister() throws Exception {
    respondAsContinue();
    RunCLI run =
        RunCLI.run(
            "--source-catalog-type",
            "HIVE",
            "--source-catalog-properties",
            "warehouse=" + warehousePath1 + ",uri=" + catalog.getConf().get("hive.metastore.uris"),
            "--target-catalog-type",
            "NESSIE",
            "--target-catalog-properties",
            "uri=" + nessieUri + ",ref=main,warehouse=" + warehousePath2);

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut())
        .contains(
            "User has not specified the table identifiers. "
                + "Selecting all the tables from all the namespaces from the source catalog.");
    Assertions.assertThat(run.getOut()).contains("Identified 4 tables for registration.");
    Assertions.assertThat(run.getOut())
        .contains(
            "Summary: \n- Successfully registered 4 tables from HIVE catalog to"
                + " NESSIE catalog.");
    Assertions.assertThat(run.getOut())
        .contains("Details: \n" + "- Successfully registered these tables:\n");
    // using the fresh instance of nessie catalog at client side to get the latest state of main
    // branch.
    catalog2 = createNessieCatalog(warehousePath2, nessieUri);
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
    respondAsContinue();
    RunCLI run =
        RunCLI.run(
            "--source-catalog-type",
            "NESSIE",
            "--source-catalog-properties",
            "uri=" + nessieUri + ",ref=main,warehouse=" + warehousePath2,
            "--target-catalog-type",
            "HIVE",
            "--target-catalog-properties",
            "warehouse=" + warehousePath1 + ",uri=" + catalog.getConf().get("hive.metastore.uris"),
            "--delete-source-tables");

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut())
        .contains(
            "User has not specified the table identifiers. "
                + "Selecting all the tables from all the namespaces from the source catalog.");
    Assertions.assertThat(run.getOut()).contains("Identified 1 tables for migration.");
    Assertions.assertThat(run.getOut())
        .contains(
            "Summary: \n- Successfully migrated 1 tables from NESSIE catalog to"
                + " HIVE catalog.");
    Assertions.assertThat(run.getOut())
        .contains("Details: \n" + "- Successfully migrated these tables:\n");
    // migrated table should be present in the target catalog
    Assertions.assertThat(catalog1.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl5"), // tbl5 is the migrated table
            TableIdentifier.parse("bar.tbl4"),
            TableIdentifier.parse("bar.tbl3"));

    // migrated table should not be there in the source catalog
    // using the fresh instance of nessie catalog at client side to get the latest state of main
    // branch.
    catalog2 = createNessieCatalog(warehousePath2, nessieUri);
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar"))).isEmpty();
  }

  private void respondAsContinue() {
    String input = "yes\n";
    ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes());
    System.setIn(in);
  }
}
