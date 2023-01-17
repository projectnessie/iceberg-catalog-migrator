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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
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
  protected static void setup() {
    try {
      startMetastore();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    warehousePath1 = catalog.getConf().get("hive.metastore.warehouse.dir");
    warehousePath2 = String.format("file://%s", warehouse2.getAbsolutePath());

    // assign to hive catalog from the parent class
    catalog1 = catalog;
    ((SupportsNamespaces) catalog1).createNamespace(Namespace.of("foo"), Collections.emptyMap());
    ((SupportsNamespaces) catalog1).createNamespace(Namespace.of("bar"), Collections.emptyMap());

    catalog2 = createHadoopCatalog(warehousePath2, "catalog2");
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
  }

  @BeforeEach
  protected void beforeEach() {
    // two tables in 'foo' namespace
    catalog1.createTable(TableIdentifier.of(Namespace.of("foo"), "tbl1"), schema);
    catalog1.createTable(TableIdentifier.of(Namespace.of("foo"), "tbl2"), schema);
    // two tables in 'bar' namespace
    catalog1.createTable(TableIdentifier.of(Namespace.of("bar"), "tbl3"), schema);
    catalog1.createTable(TableIdentifier.of(Namespace.of("bar"), "tbl4"), schema);
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
    RunCLI run =
        RunCLI.run(
            "HIVE",
            "warehouse=" + warehousePath1 + ",uri=" + catalog.getConf().get("hive.metastore.uris"),
            "HADOOP",
            "warehouse=" + warehousePath2 + ",type=hadoop");

    Assertions.assertEquals(0, run.getExitCode());
    Assertions.assertTrue(
        run.getOut()
            .contains(
                "User has not specified the table identifiers. "
                    + "Selecting all the tables from all the namespaces from the source catalog."));
    Assertions.assertTrue(run.getOut().contains("Identified 4 tables for registration."));
    Assertions.assertTrue(
        run.getOut()
            .contains(
                "Summary: \n- Successfully registered 4 tables from HIVE catalog to"
                    + " HADOOP catalog."));
    Assertions.assertTrue(
        run.getOut()
            .contains(
                "Details: \n- Successfully registered these tables: \n"
                    + "[bar.tbl3, bar.tbl4, foo.tbl1, foo.tbl2]"));
  }
}
