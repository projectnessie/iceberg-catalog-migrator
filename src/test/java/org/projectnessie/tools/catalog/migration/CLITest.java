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
import static org.projectnessie.tools.catalog.migration.CatalogMigrationCLI.FAILED_IDENTIFIERS_FILE;

import java.io.ByteArrayInputStream;
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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
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

    // to handle the user prompt
    respondAsContinue();
  }

  private void respondAsContinue() {
    String input = "yes\n";
    ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes());
    System.setIn(in);
  }

  private void respondAsAbort() {
    String input = "no\n";
    ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes());
    System.setIn(in);
  }

  private void respondDummy() {
    String input = "dummy\n";
    ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes());
    System.setIn(in);
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
    RunCLI run = runWithDefaultArgs();

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut())
        .contains(
            "User has not specified the table identifiers. "
                + "Selecting all the tables from all the namespaces from the source catalog.");
    Assertions.assertThat(run.getOut()).contains("Identified 4 tables for registration.");
    Assertions.assertThat(run.getOut())
        .contains(
            "Summary: \n- Successfully registered 4 tables from HADOOP catalog to"
                + " HADOOP catalog.");
    Assertions.assertThat(run.getOut())
        .contains("Details: \n" + "- Successfully registered these tables:\n");
    Assertions.assertThat(catalog2.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl-1"), TableIdentifier.parse("foo.tbl-2"));
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl-3"), TableIdentifier.parse("bar.tbl-4"));
  }

  @Test
  @Order(1)
  public void testMigrate() throws Exception {
    RunCLI run =
        RunCLI.run(
            "--source-catalog-type",
            "HADOOP",
            "--source-catalog-properties",
            "warehouse=" + warehousePath1 + ",type=hadoop",
            "--target-catalog-type",
            "HADOOP",
            "--target-catalog-properties",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "--delete-source-tables");

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    // note that keywords in output is "migrate" instead of "register".
    // If the catalog was not hadoop catalog, tables also should get deleted from the source catalog
    // after migration.
    Assertions.assertThat(run.getOut())
        .contains(
            "User has not specified the table identifiers. "
                + "Selecting all the tables from all the namespaces from the source catalog.");
    Assertions.assertThat(run.getOut()).contains("Identified 4 tables for migration.");
    Assertions.assertThat(run.getOut())
        .contains(
            "Summary: \n- Successfully migrated 4 tables from HADOOP catalog to"
                + " HADOOP catalog.");
    Assertions.assertThat(run.getOut())
        .contains("Details: \n" + "- Successfully migrated these tables:\n");
    Assertions.assertThat(catalog2.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl-1"), TableIdentifier.parse("foo.tbl-2"));
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl-3"), TableIdentifier.parse("bar.tbl-4"));
  }

  @Test
  @Order(2)
  public void testRegisterSelectedTables() throws Exception {
    // using `--identifiers` option
    RunCLI run =
        RunCLI.run(
            "--source-catalog-type",
            "HADOOP",
            "--source-catalog-properties",
            "warehouse=" + warehousePath1 + ",type=hadoop",
            "--target-catalog-type",
            "HADOOP",
            "--target-catalog-properties",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "--identifiers",
            "bar.tbl-3");

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut())
        .doesNotContain(
            "User has not specified the table identifiers. "
                + "Selecting all the tables from all the namespaces from the source catalog.");

    Assertions.assertThat(run.getOut()).contains("Identified 1 tables for registration.");
    Assertions.assertThat(run.getOut())
        .contains(
            "Summary: \n- Successfully registered 1 tables from HADOOP catalog to"
                + " HADOOP catalog.");
    Assertions.assertThat(run.getOut())
        .contains("Details: \n- Successfully registered these tables:\n" + "[bar.tbl-3]");

    // using `--identifiers-from-file` option
    respondAsContinue();
    Path identifierFile = Paths.get("ids.txt");
    Files.write(identifierFile, Collections.singletonList("bar.tbl-4"));
    run =
        RunCLI.run(
            "--source-catalog-type",
            "HADOOP",
            "--source-catalog-properties",
            "warehouse=" + warehousePath1 + ",type=hadoop",
            "--target-catalog-type",
            "HADOOP",
            "--target-catalog-properties",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "--identifiers-from-file",
            "ids.txt");
    Files.delete(identifierFile);

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut()).contains("Collecting identifiers from the file ids.txt...");

    Assertions.assertThat(run.getOut())
        .doesNotContain(
            "User has not specified the table identifiers. "
                + "Selecting all the tables from all the namespaces from the source catalog.");

    Assertions.assertThat(run.getOut()).contains("Identified 1 tables for registration.");
    Assertions.assertThat(run.getOut())
        .contains(
            "Summary: \n- Successfully registered 1 tables from HADOOP catalog to"
                + " HADOOP catalog.");
    Assertions.assertThat(run.getOut())
        .contains("Details: \n- Successfully registered these tables:\n" + "[bar.tbl-4]");

    // using --identifiers-regex option which matches all the tables starts with "foo."
    respondAsContinue();
    run =
        RunCLI.run(
            "--source-catalog-type",
            "HADOOP",
            "--source-catalog-properties",
            "warehouse=" + warehousePath1 + ",type=hadoop",
            "--target-catalog-type",
            "HADOOP",
            "--target-catalog-properties",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "--identifiers-regex",
            "^foo\\..*");
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut())
        .contains(
            "User has not specified the table identifiers. Selecting all the tables from all the namespaces "
                + "from the source catalog which matches the regex pattern:^foo\\..*");

    Assertions.assertThat(run.getOut())
        .contains(
            "Collecting all the tables from all the namespaces of source catalog "
                + "which matches the regex pattern:^foo\\..*");

    Assertions.assertThat(run.getOut()).contains("Identified 2 tables for registration.");
    Assertions.assertThat(run.getOut())
        .contains(
            "Summary: \n- Successfully registered 2 tables from HADOOP catalog to"
                + " HADOOP catalog.");
    Assertions.assertThat(run.getOut())
        .contains("Details: \n" + "- Successfully registered these tables:\n");
    Assertions.assertThat(catalog2.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl-1"), TableIdentifier.parse("foo.tbl-2"));
  }

  @Test
  @Order(3)
  public void testRegisterError() throws Exception {
    // use invalid namespace which leads to NoSuchTableException
    RunCLI run =
        RunCLI.run(
            "--source-catalog-type",
            "HADOOP",
            "--source-catalog-properties",
            "warehouse=" + warehousePath1 + ",type=hadoop",
            "--target-catalog-type",
            "HADOOP",
            "--target-catalog-properties",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "--identifiers",
            "dummy.tbl-3");
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut()).contains("Identified 1 tables for registration.");
    Assertions.assertThat(run.getOut())
        .contains(
            "Summary: \n- Failed to register 1 tables from HADOOP catalog to HADOOP catalog."
                + " Please check the `catalog_migration.log`");
    Assertions.assertThat(run.getOut())
        .contains("Details: \n- Failed to register these tables:\n[dummy.tbl-3]");

    // try to register same table twice which leads to AlreadyExistsException
    respondAsContinue();
    RunCLI.run(
        "--source-catalog-type",
        "HADOOP",
        "--source-catalog-properties",
        "warehouse=" + warehousePath1 + ",type=hadoop",
        "--target-catalog-type",
        "HADOOP",
        "--target-catalog-properties",
        "warehouse=" + warehousePath2 + ",type=hadoop",
        "--identifiers",
        "foo.tbl-2");
    respondAsContinue();
    run =
        RunCLI.run(
            "--source-catalog-type",
            "HADOOP",
            "--source-catalog-properties",
            "warehouse=" + warehousePath1 + ",type=hadoop",
            "--target-catalog-type",
            "HADOOP",
            "--target-catalog-properties",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "--identifiers",
            "foo.tbl-2");
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut()).contains("Identified 1 tables for registration.");
    Assertions.assertThat(run.getOut())
        .contains(
            "Summary: \n- Failed to register 1 tables from HADOOP catalog to HADOOP catalog."
                + " Please check the `catalog_migration.log`");
    Assertions.assertThat(run.getOut())
        .contains("Details: \n- Failed to register these tables:\n[foo.tbl-2]");
  }

  @Test
  @Order(4)
  public void testRegisterWithFewFailures() throws Exception {
    // register only foo.tbl-2
    RunCLI run =
        RunCLI.run(
            "--source-catalog-type",
            "HADOOP",
            "--source-catalog-properties",
            "warehouse=" + warehousePath1 + ",type=hadoop",
            "--target-catalog-type",
            "HADOOP",
            "--target-catalog-properties",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "--identifiers",
            "foo.tbl-2");
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut()).contains("Identified 1 tables for registration.");
    Assertions.assertThat(run.getOut())
        .contains(
            "Summary: \n- Successfully registered 1 tables from HADOOP catalog to HADOOP catalog.");
    Assertions.assertThat(run.getOut())
        .contains("Details: \n" + "- Successfully registered these tables:\n" + "[foo.tbl-2]");

    // register all the tables from source catalog again
    respondAsContinue();
    run =
        RunCLI.run(
            "--source-catalog-type",
            "HADOOP",
            "--source-catalog-properties",
            "warehouse=" + warehousePath1 + ",type=hadoop",
            "--target-catalog-type",
            "HADOOP",
            "--target-catalog-properties",
            "warehouse=" + warehousePath2 + ",type=hadoop");
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut()).contains("Identified 4 tables for registration.");
    Assertions.assertThat(run.getOut())
        .contains(
            "Summary: \n"
                + "- Successfully registered 3 tables from HADOOP catalog to HADOOP catalog.\n"
                + "- Failed to register 1 tables from HADOOP catalog to HADOOP catalog. "
                + "Please check the `catalog_migration.log` file for the failure reason. \n"
                + "Failed identifiers are written into `failed_identifiers.txt`. "
                + "Retry with that file using `--identifiers-from-file` option "
                + "if the failure is because of network/connection timeouts.");
    Assertions.assertThat(run.getOut())
        .contains("Details: \n" + "- Successfully registered these tables:\n");
    Assertions.assertThat(run.getOut()).contains("- Failed to register these tables:\n[foo.tbl-2]");
    Assertions.assertThat(catalog2.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl-1"), TableIdentifier.parse("foo.tbl-2"));
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl-3"), TableIdentifier.parse("bar.tbl-4"));

    // retry the failed tables using --identifiers-from-file
    respondAsContinue();
    run =
        RunCLI.run(
            "--source-catalog-type",
            "HADOOP",
            "--source-catalog-properties",
            "warehouse=" + warehousePath1 + ",type=hadoop",
            "--target-catalog-type",
            "HADOOP",
            "--target-catalog-properties",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "--identifiers-from-file",
            FAILED_IDENTIFIERS_FILE);
    Assertions.assertThat(run.getOut())
        .contains(
            "Summary: \n"
                + "- Failed to register 1 tables from HADOOP catalog to HADOOP catalog. "
                + "Please check the `catalog_migration.log` file for the failure reason. \n"
                + "Failed identifiers are written into `failed_identifiers.txt`. "
                + "Retry with that file using `--identifiers-from-file` option "
                + "if the failure is because of network/connection timeouts.");
    Assertions.assertThat(run.getOut())
        .contains("Details: \n" + "- Failed to register these tables:\n" + "[foo.tbl-2]");
  }

  @Test
  @Order(5)
  public void testRegisterNoTables() throws Exception {
    // source catalog is catalog2 which has no tables.
    RunCLI run =
        RunCLI.run(
            "--source-catalog-type",
            "HADOOP",
            "--source-catalog-properties",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "--target-catalog-type",
            "HADOOP",
            "--target-catalog-properties",
            "warehouse=" + warehousePath1 + ",type=hadoop");

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut()).contains("Identified 0 tables for registration.");
  }

  @Test
  @Order(6)
  public void testPrompt() throws Exception {
    respondAsAbort();
    RunCLI run = runWithDefaultArgs();
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    // should abort
    Assertions.assertThat(run.getOut()).contains("Aborting...");
    // should not have other messages
    Assertions.assertThat(run.getOut()).doesNotContain("Summary");

    respondDummy();
    run = runWithDefaultArgs();
    Assertions.assertThat(run.getExitCode()).isEqualTo(1);
    Assertions.assertThat(run.getOut()).contains("Invalid input. Please enter 'yes' or 'no'.");

    respondAsContinue();
    run = runWithDefaultArgs();
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    // should abort
    Assertions.assertThat(run.getOut()).contains("Continuing...");
    Assertions.assertThat(run.getOut()).contains("Summary");
  }

  @Test
  @Order(7)
  public void testDryRun() throws Exception {
    RunCLI run =
        RunCLI.run(
            "--source-catalog-type",
            "HADOOP",
            "--source-catalog-properties",
            "warehouse=" + warehousePath1 + ",type=hadoop",
            "--target-catalog-type",
            "HADOOP",
            "--target-catalog-properties",
            "warehouse=" + warehousePath2 + ",type=hadoop",
            "--dry-run");

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    // should not prompt for dry run
    Assertions.assertThat(run.getOut())
        .doesNotContain(
            "Have you read the above warnings and are you sure you want to continue? (yes/no):");
    Assertions.assertThat(run.getOut()).contains("Dry run is completed.");
    Assertions.assertThat(run.getOut())
        .contains(
            "Summary: \n"
                + "- Identified 4 tables for registration by dry-run. "
                + "These identifiers are also written into dry_run_identifiers.txt. "
                + "You can use this file with `--identifiers-from-file` option.");
    Assertions.assertThat(run.getOut())
        .contains("Details: \n" + "- Identified these tables for registration by dry-run:\n");
  }

  @Test
  @Order(9)
  public void version() throws Exception {
    RunCLI run = RunCLI.run("--version");
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut()).startsWith(System.getProperty("expectedCLIVersion"));
  }

  private RunCLI runWithDefaultArgs() throws Exception {
    return RunCLI.run(
        "--source-catalog-type",
        "HADOOP",
        "--source-catalog-properties",
        "warehouse=" + warehousePath1 + ",type=hadoop",
        "--target-catalog-type",
        "HADOOP",
        "--target-catalog-properties",
        "warehouse=" + warehousePath2 + ",type=hadoop");
  }
}
