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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class CLITest {

  private static @TempDir File warehouse1;
  private static String warehousePath1;

  private static @TempDir File warehouse2;
  private static String warehousePath2;

  private static Catalog catalog1;

  private static Catalog catalog2;

  private static final Schema schema =
      new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());

  @BeforeAll
  protected static void setup() {
    warehousePath1 = String.format("file://%s", warehouse1.getAbsolutePath());
    warehousePath2 = String.format("file://%s", warehouse2.getAbsolutePath());

    catalog1 = createCatalog(warehousePath1, "catalog1");
    ((HadoopCatalog) catalog1).createNamespace(Namespace.of("foo"), Collections.emptyMap());
    ((HadoopCatalog) catalog1).createNamespace(Namespace.of("bar"), Collections.emptyMap());

    catalog2 = createCatalog(warehousePath2, "catalog2");
    ((HadoopCatalog) catalog2).createNamespace(Namespace.of("foo"), Collections.emptyMap());
    ((HadoopCatalog) catalog2).createNamespace(Namespace.of("bar"), Collections.emptyMap());
  }

  @BeforeEach
  protected void beforeEach() {
    // two tables in 'foo' namespace
    catalog1.createTable(TableIdentifier.of(Namespace.of("foo"), "tbl-1"), schema);
    catalog1.createTable(TableIdentifier.of(Namespace.of("foo"), "tbl-2"), schema);
    // two tables in 'bar' namespace
    catalog1.createTable(TableIdentifier.of(Namespace.of("bar"), "tbl-3"), schema);
    catalog1.createTable(TableIdentifier.of(Namespace.of("bar"), "tbl-4"), schema);
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

  private static Catalog createCatalog(String warehousePath, String name) {
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
            "HADOOP",
            "warehouse=" + warehousePath1 + ",type=hadoop",
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
                "Summary: \n- Successfully registered 4 tables from HADOOP catalog to"
                    + " HADOOP catalog."));
    Assertions.assertTrue(
        run.getOut()
            .contains(
                "Details: \n- Successfully registered these tables: \n"
                    + "[foo.tbl-1, foo.tbl-2, bar.tbl-4, bar.tbl-3]"));
  }

  @Test
  @Order(1)
  public void testMigrate() throws Exception {
    RunCLI run =
        RunCLI.run(
            "HADOOP",
            "warehouse=" + warehousePath1 + ",type=hadoop",
            "HADOOP",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "--delete-source-tables");

    Assertions.assertEquals(0, run.getExitCode());
    // note that keywords in output is "migrate" instead of "register".
    // If the catalog was not hadoop catalog, tables also should get deleted from the source catalog
    // after migration.
    Assertions.assertTrue(
        run.getOut()
            .contains(
                "User has not specified the table identifiers. "
                    + "Selecting all the tables from all the namespaces from the source catalog."));
    Assertions.assertTrue(run.getOut().contains("Identified 4 tables for migration."));
    Assertions.assertTrue(
        run.getOut()
            .contains(
                "Summary: \n- Successfully migrated 4 tables from HADOOP catalog to"
                    + " HADOOP catalog."));
    Assertions.assertTrue(
        run.getOut()
            .contains(
                "Details: \n- Successfully migrated these tables: \n"
                    + "[foo.tbl-1, foo.tbl-2, bar.tbl-4, bar.tbl-3]"));
  }

  @Test
  @Order(2)
  public void testRegisterSelectedTables() throws Exception {
    // using `--identifiers` option
    RunCLI run =
        RunCLI.run(
            "HADOOP",
            "warehouse=" + warehousePath1 + ",type=hadoop",
            "HADOOP",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "--identifiers",
            "bar.tbl-3");

    Assertions.assertEquals(0, run.getExitCode());

    Assertions.assertFalse(
        run.getOut()
            .contains(
                "User has not specified the table identifiers. "
                    + "Selecting all the tables from all the namespaces from the source catalog."));

    Assertions.assertTrue(run.getOut().contains("Identified 1 tables for registration."));
    Assertions.assertTrue(
        run.getOut()
            .contains(
                "Summary: \n- Successfully registered 1 tables from HADOOP catalog to"
                    + " HADOOP catalog."));
    Assertions.assertTrue(
        run.getOut()
            .contains("Details: \n- Successfully registered these tables: \n" + "[bar.tbl-3]"));

    // using `--identifiers-from-file` option
    Path identifierFile = Paths.get("ids.txt");
    Files.write(identifierFile, Collections.singletonList("foo.tbl-2"));
    run =
        RunCLI.run(
            "HADOOP",
            "warehouse=" + warehousePath1 + ",type=hadoop",
            "HADOOP",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "--identifiers-from-file",
            "ids.txt");
    Files.delete(identifierFile);

    Assertions.assertEquals(0, run.getExitCode());

    Assertions.assertTrue(run.getOut().contains("Collecting identifiers from the file ids.txt..."));

    Assertions.assertFalse(
        run.getOut()
            .contains(
                "User has not specified the table identifiers. "
                    + "Selecting all the tables from all the namespaces from the source catalog."));

    Assertions.assertTrue(run.getOut().contains("Identified 1 tables for registration."));
    Assertions.assertTrue(
        run.getOut()
            .contains(
                "Summary: \n- Successfully registered 1 tables from HADOOP catalog to"
                    + " HADOOP catalog."));
    Assertions.assertTrue(
        run.getOut()
            .contains("Details: \n- Successfully registered these tables: \n" + "[foo.tbl-2]"));
  }

  @Test
  @Order(3)
  public void testRegisterMultiThread() throws Exception {
    RunCLI run =
        RunCLI.run(
            "HADOOP",
            "warehouse=" + warehousePath1 + ",type=hadoop",
            "HADOOP",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "-T",
            "4");

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
                "Summary: \n- Successfully registered 4 tables from HADOOP catalog to"
                    + " HADOOP catalog."));
  }

  @Test
  @Order(4)
  public void testRegisterError() throws Exception {
    // use invalid namespace which leads to NoSuchTableException
    RunCLI run =
        RunCLI.run(
            "HADOOP",
            "warehouse=" + warehousePath1 + ",type=hadoop",
            "HADOOP",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "-I",
            "dummy.tbl-3");
    Assertions.assertEquals(0, run.getExitCode());
    Assertions.assertTrue(run.getOut().contains("Identified 1 tables for registration."));
    Assertions.assertTrue(
        run.getOut()
            .contains(
                "Summary: \n- Failed to register 1 tables from HADOOP catalog to HADOOP catalog."
                    + " Please check the `catalog_migration.log`"));
    Assertions.assertTrue(
        run.getOut().contains("Details: \n- Failed to register these tables: \n[dummy.tbl-3]"));

    // try to register same table twice which leads to AlreadyExistsException
    RunCLI.run(
        "HADOOP",
        "warehouse=" + warehousePath1 + ",type=hadoop",
        "HADOOP",
        "warehouse=" + warehousePath2 + ",type=hadoop",
        "-I",
        "foo.tbl-2");
    run =
        RunCLI.run(
            "HADOOP",
            "warehouse=" + warehousePath1 + ",type=hadoop",
            "HADOOP",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "-I",
            "foo.tbl-2");
    Assertions.assertEquals(0, run.getExitCode());
    Assertions.assertTrue(run.getOut().contains("Identified 1 tables for registration."));
    Assertions.assertTrue(
        run.getOut()
            .contains(
                "Summary: \n- Failed to register 1 tables from HADOOP catalog to HADOOP catalog."
                    + " Please check the `catalog_migration.log`"));
    Assertions.assertTrue(
        run.getOut().contains("Details: \n- Failed to register these tables: \n[foo.tbl-2]"));
  }

  @Test
  @Order(5)
  public void testRegisterPartialTables() throws Exception {
    // register only foo.tbl-2
    RunCLI run =
        RunCLI.run(
            "HADOOP",
            "warehouse=" + warehousePath1 + ",type=hadoop",
            "HADOOP",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "-I",
            "foo.tbl-2");
    Assertions.assertEquals(0, run.getExitCode());
    Assertions.assertTrue(run.getOut().contains("Identified 1 tables for registration."));
    Assertions.assertTrue(
        run.getOut()
            .contains(
                "Summary: \n- Successfully registered 1 tables from HADOOP catalog to HADOOP catalog."));
    Assertions.assertTrue(
        run.getOut()
            .contains(
                "Details: \n" + "- Successfully registered these tables: \n" + "[foo.tbl-2]"));

    // register all the tables from source catalog again
    run =
        RunCLI.run(
            "HADOOP",
            "warehouse=" + warehousePath1 + ",type=hadoop",
            "HADOOP",
            "warehouse=" + warehousePath2 + ",type=hadoop");
    Assertions.assertEquals(0, run.getExitCode());
    Assertions.assertTrue(run.getOut().contains("Identified 4 tables for registration."));
    Assertions.assertTrue(
        run.getOut()
            .contains(
                "Summary: \n"
                    + "- Successfully registered 3 tables from HADOOP catalog to HADOOP catalog. \n"
                    + "- Failed to register 1 tables from HADOOP catalog to HADOOP catalog. "
                    + "Please check the `catalog_migration.log` file for the failure reason. \n"
                    + " Failed Identifiers are written to `failed_identifiers.txt`. "
                    + "Retry with that file using `--identifiers-from-file` option "
                    + "if the failure is because of network/connection timeouts."));
    Assertions.assertTrue(
        run.getOut()
            .contains(
                "Details: \n"
                    + "- Successfully registered these tables: \n"
                    + "[foo.tbl-1, bar.tbl-4, bar.tbl-3]\n"
                    + "- Failed to register these tables: \n"
                    + "[foo.tbl-2]"));

    // retry the failed tables using --identifiers-from-file
    run =
        RunCLI.run(
            "HADOOP",
            "warehouse=" + warehousePath1 + ",type=hadoop",
            "HADOOP",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "--identifiers-from-file",
            "failed_identifiers.txt");
    Assertions.assertTrue(
        run.getOut()
            .contains(
                "Summary: \n"
                    + "- Failed to register 1 tables from HADOOP catalog to HADOOP catalog. "
                    + "Please check the `catalog_migration.log` file for the failure reason. \n"
                    + " Failed Identifiers are written to `failed_identifiers.txt`. "
                    + "Retry with that file using `--identifiers-from-file` option if the failure is because of network/connection timeouts.\n"
                    + "\n"
                    + "Details: \n"
                    + "- Failed to register these tables: \n"
                    + "[foo.tbl-2]"));
  }

  @Test
  @Order(6)
  public void testRegisterNoTables() throws Exception {
    // source catalog is catalog2 which has no tables.
    RunCLI run =
        RunCLI.run(
            "HADOOP",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "HADOOP",
            "warehouse=" + warehousePath1 + ",type=hadoop");

    Assertions.assertEquals(0, run.getExitCode());
    Assertions.assertTrue(run.getOut().contains("Identified 0 tables for registration."));
  }

  @Test
  @Order(7)
  public void version() throws Exception {
    RunCLI run = RunCLI.run("--version");
    Assertions.assertEquals(0, run.getExitCode());
    Assertions.assertTrue(run.getOut().startsWith(System.getProperty("expectedCLIVersion")));
  }
}
