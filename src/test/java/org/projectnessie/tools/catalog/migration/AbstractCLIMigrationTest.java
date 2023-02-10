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

import com.google.common.collect.Lists;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public abstract class AbstractCLIMigrationTest extends AbstractTest {

  protected static @TempDir File warehouse1;

  protected static @TempDir File warehouse2;

  protected static @TempDir File outputDir;

  protected static String dryRunFile;
  protected static String failedIdentifiersFile;

  protected static String sourceCatalogProperties;
  protected static String targetCatalogProperties;

  protected static String sourceCatalogType;
  protected static String targetCatalogType;

  @BeforeEach
  protected void beforeEach() {
    createTables();
  }

  @AfterEach
  protected void afterEach() {
    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    // create table will call refresh internally.
    catalog1.createTable(TableIdentifier.of(Namespace.of("bar"), "tblx"), schema).refresh();
    catalog2.createTable(TableIdentifier.of(Namespace.of("bar"), "tblx"), schema).refresh();

    dropTables();
    deleteFileIfExists(dryRunFile);
    deleteFileIfExists(failedIdentifiersFile);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegister(boolean deleteSourceTables) throws Exception {
    RunCLI run = RunCLI.runWithContinue(registerAllTablesArgs(deleteSourceTables));

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut())
        .contains(
            "User has not specified the table identifiers. "
                + "Selecting all the tables from all the namespaces from the source catalog.");
    String operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 4 tables for %s.", operation));
    operation = deleteSourceTables ? "migrated" : "registered";
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: \n- Successfully %s 4 tables from %s catalog to %s catalog.",
                operation, sourceCatalogType, targetCatalogType));
    Assertions.assertThat(run.getOut())
        .contains(String.format("Details: \n" + "- Successfully %s these tables:\n", operation));

    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    catalog2.loadTable(TableIdentifier.parse("foo.tbl1")).refresh();

    Assertions.assertThat(catalog2.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl3"), TableIdentifier.parse("bar.tbl4"));
  }

  @Order(1)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterSelectedTables(boolean deleteSourceTables) throws Exception {
    // using `--identifiers` option
    RunCLI run =
        registerTablesCLI(
            deleteSourceTables,
            "--source-catalog-type",
            sourceCatalogType,
            "--source-catalog-properties",
            sourceCatalogProperties,
            "--target-catalog-type",
            targetCatalogType,
            "--target-catalog-properties",
            targetCatalogProperties,
            "--identifiers",
            "bar.tbl3",
            "--output-dir",
            outputDir.getAbsolutePath());

    Assertions.assertThat(run.getOut())
        .doesNotContain(
            "User has not specified the table identifiers. "
                + "Selecting all the tables from all the namespaces from the source catalog.");
    String operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 1 tables for %s.", operation));
    operation = deleteSourceTables ? "migrated" : "registered";
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: \n- Successfully %s 1 tables from %s catalog to" + " %s catalog.",
                operation, sourceCatalogType, targetCatalogType));
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Details: \n- Successfully %s these tables:\n" + "[bar.tbl3]", operation));

    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    catalog2.loadTable(TableIdentifier.parse("bar.tbl3")).refresh();

    Assertions.assertThat(catalog2.listTables(Namespace.of("foo"))).isEmpty();
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar")))
        .containsExactly(TableIdentifier.parse("bar.tbl3"));

    // using `--identifiers-from-file` option
    Path identifierFile = Paths.get("ids.txt");
    Files.write(identifierFile, Collections.singletonList("bar.tbl4"));
    run =
        registerTablesCLI(
            deleteSourceTables,
            "--source-catalog-type",
            sourceCatalogType,
            "--source-catalog-properties",
            sourceCatalogProperties,
            "--target-catalog-type",
            targetCatalogType,
            "--target-catalog-properties",
            targetCatalogProperties,
            "--identifiers-from-file",
            "ids.txt",
            "--output-dir",
            outputDir.getAbsolutePath());
    Files.delete(identifierFile);

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut())
        .doesNotContain(
            "User has not specified the table identifiers. "
                + "Selecting all the tables from all the namespaces from the source catalog.");
    operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 1 tables for %s.", operation));
    operation = deleteSourceTables ? "migrated" : "registered";
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: \n- Successfully %s 1 tables from %s catalog to" + " %s catalog.",
                operation, sourceCatalogType, targetCatalogType));
    Assertions.assertThat(run.getOut())
        .contains(String.format("Details: \n" + "- Successfully %s these tables:\n", operation));

    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    catalog2.loadTable(TableIdentifier.parse("bar.tbl3")).refresh();

    Assertions.assertThat(catalog2.listTables(Namespace.of("foo"))).isEmpty();
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl4"), TableIdentifier.parse("bar.tbl3"));

    // using --identifiers-regex option which matches all the tables starts with "foo."
    run =
        registerTablesCLI(
            deleteSourceTables,
            "--source-catalog-type",
            sourceCatalogType,
            "--source-catalog-properties",
            sourceCatalogProperties,
            "--target-catalog-type",
            targetCatalogType,
            "--target-catalog-properties",
            targetCatalogProperties,
            "--identifiers-regex",
            "^foo\\..*",
            "--output-dir",
            outputDir.getAbsolutePath());
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut())
        .contains(
            "User has not specified the table identifiers. Selecting all the tables from all the namespaces "
                + "from the source catalog which matches the regex pattern:^foo\\..*");
    Assertions.assertThat(run.getOut())
        .contains(
            "Collecting all the tables from all the namespaces of source catalog "
                + "which matches the regex pattern:^foo\\..*");
    operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 2 tables for %s.", operation));
    operation = deleteSourceTables ? "migrated" : "registered";
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: \n- Successfully %s 2 tables from %s catalog to" + " %s catalog.",
                operation, sourceCatalogType, targetCatalogType));
    Assertions.assertThat(run.getOut())
        .contains(String.format("Details: \n" + "- Successfully %s these tables:\n", operation));

    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    catalog2.loadTable(TableIdentifier.parse("bar.tbl3")).refresh();

    Assertions.assertThat(catalog2.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl3"), TableIdentifier.parse("bar.tbl4"));
  }

  @Order(2)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterError(boolean deleteSourceTables) throws Exception {
    // use invalid namespace which leads to NoSuchTableException
    RunCLI run =
        registerTablesCLI(
            deleteSourceTables,
            "--source-catalog-type",
            sourceCatalogType,
            "--source-catalog-properties",
            sourceCatalogProperties,
            "--target-catalog-type",
            targetCatalogType,
            "--target-catalog-properties",
            targetCatalogProperties,
            "--identifiers",
            "dummy.tbl3",
            "--output-dir",
            outputDir.getAbsolutePath());
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    String operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 1 tables for %s.", operation));
    operation = deleteSourceTables ? "migrate" : "register";
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: \n- Failed to %s 1 tables from %s catalog to %s catalog."
                    + " Please check the `catalog_migration.log`",
                operation, sourceCatalogType, targetCatalogType));
    Assertions.assertThat(run.getOut())
        .contains(
            String.format("Details: \n- Failed to %s these tables:\n[dummy.tbl3]", operation));

    // try to register same table twice which leads to AlreadyExistsException
    registerTablesCLI(
        deleteSourceTables,
        "--source-catalog-type",
        sourceCatalogType,
        "--source-catalog-properties",
        sourceCatalogProperties,
        "--target-catalog-type",
        targetCatalogType,
        "--target-catalog-properties",
        targetCatalogProperties,
        "--identifiers",
        "foo.tbl2",
        "--output-dir",
        outputDir.getAbsolutePath());
    run =
        registerTablesCLI(
            deleteSourceTables,
            "--source-catalog-type",
            sourceCatalogType,
            "--source-catalog-properties",
            sourceCatalogProperties,
            "--target-catalog-type",
            targetCatalogType,
            "--target-catalog-properties",
            targetCatalogProperties,
            "--identifiers",
            "foo.tbl2",
            "--output-dir",
            outputDir.getAbsolutePath());
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 1 tables for %s.", operation));
    operation = deleteSourceTables ? "migrate" : "register";
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: \n- Failed to %s 1 tables from %s catalog to %s catalog."
                    + " Please check the `catalog_migration.log`",
                operation, sourceCatalogType, targetCatalogType));
    Assertions.assertThat(run.getOut())
        .contains(String.format("Details: \n- Failed to %s these tables:\n[foo.tbl2]", operation));
  }

  @Order(3)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterWithFewFailures(boolean deleteSourceTables) throws Exception {
    // register only foo.tbl2
    RunCLI run =
        registerTablesCLI(
            deleteSourceTables,
            "--source-catalog-type",
            sourceCatalogType,
            "--source-catalog-properties",
            sourceCatalogProperties,
            "--target-catalog-type",
            targetCatalogType,
            "--target-catalog-properties",
            targetCatalogProperties,
            "--identifiers",
            "foo.tbl2",
            "--output-dir",
            outputDir.getAbsolutePath());
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    String operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 1 tables for %s.", operation));
    operation = deleteSourceTables ? "migrated" : "registered";
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: \n- Successfully %s 1 tables from %s catalog to %s catalog.",
                operation, sourceCatalogType, targetCatalogType));
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Details: \n" + "- Successfully %s these tables:\n" + "[foo.tbl2]", operation));

    if (deleteSourceTables && !(catalog1 instanceof HadoopCatalog)) {
      // create a table with the same name in source catalog which got deleted.
      catalog1.createTable(TableIdentifier.of(Namespace.of("foo"), "tbl2"), schema);
    }

    // register all the tables from source catalog again
    run =
        registerTablesCLI(
            deleteSourceTables,
            "--source-catalog-type",
            sourceCatalogType,
            "--source-catalog-properties",
            sourceCatalogProperties,
            "--target-catalog-type",
            targetCatalogType,
            "--target-catalog-properties",
            targetCatalogProperties,
            "--output-dir",
            outputDir.getAbsolutePath());
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 4 tables for %s.", operation));
    operation = deleteSourceTables ? "migrated" : "registered";
    String ops = deleteSourceTables ? "migrate" : "register";
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: \n"
                    + "- Successfully %s 3 tables from %s catalog to %s catalog.\n"
                    + "- Failed to %s 1 tables from %s catalog to %s catalog. "
                    + "Please check the `catalog_migration.log` file for the failure reason. \n"
                    + "Failed identifiers are written into `failed_identifiers.txt`. "
                    + "Retry with that file using `--identifiers-from-file` option "
                    + "if the failure is because of network/connection timeouts.",
                operation,
                sourceCatalogType,
                targetCatalogType,
                ops,
                sourceCatalogType,
                targetCatalogType));
    Assertions.assertThat(run.getOut())
        .contains(String.format("Details: \n" + "- Successfully %s these tables:\n", operation));
    Assertions.assertThat(run.getOut())
        .contains(String.format("- Failed to %s these tables:\n[foo.tbl2]", ops));

    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    catalog2.loadTable(TableIdentifier.parse("bar.tbl3")).refresh();

    Assertions.assertThat(catalog2.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl3"), TableIdentifier.parse("bar.tbl4"));

    // retry the failed tables using --identifiers-from-file
    run =
        registerTablesCLI(
            deleteSourceTables,
            "--source-catalog-type",
            sourceCatalogType,
            "--source-catalog-properties",
            sourceCatalogProperties,
            "--target-catalog-type",
            targetCatalogType,
            "--target-catalog-properties",
            targetCatalogProperties,
            "--identifiers-from-file",
            failedIdentifiersFile,
            "--output-dir",
            outputDir.getAbsolutePath());
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: \n"
                    + "- Failed to %s 1 tables from %s catalog to %s catalog. "
                    + "Please check the `catalog_migration.log` file for the failure reason. \n"
                    + "Failed identifiers are written into `failed_identifiers.txt`. "
                    + "Retry with that file using `--identifiers-from-file` option "
                    + "if the failure is because of network/connection timeouts.",
                ops, sourceCatalogType, targetCatalogType));
    Assertions.assertThat(run.getOut())
        .contains(
            String.format("Details: \n" + "- Failed to %s these tables:\n" + "[foo.tbl2]", ops));
    Assertions.assertThat(new File(failedIdentifiersFile).exists()).isTrue();
    Assertions.assertThat(Files.readAllLines(Paths.get(failedIdentifiersFile)))
        .containsExactly("foo.tbl2");
  }

  @Order(4)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterNoTables(boolean deleteSourceTables) throws Exception {
    // source catalog is catalog2 which has no tables.
    RunCLI run =
        registerTablesCLI(
            deleteSourceTables,
            "--source-catalog-type",
            targetCatalogType,
            "--source-catalog-properties",
            targetCatalogProperties,
            "--target-catalog-type",
            sourceCatalogType,
            "--target-catalog-properties",
            sourceCatalogProperties,
            "--output-dir",
            outputDir.getAbsolutePath());

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    String operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 0 tables for %s.", operation));
  }

  @Order(5)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPrompt(boolean deleteSourceTables) throws Exception {
    RunCLI run = RunCLI.runWithAbort(registerAllTablesArgs(deleteSourceTables));
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    // should abort
    Assertions.assertThat(run.getOut()).contains("Aborting...");
    // should not have other messages
    Assertions.assertThat(run.getOut()).doesNotContain("Summary");

    run = RunCLI.runWithDummyInput(registerAllTablesArgs(deleteSourceTables));
    Assertions.assertThat(run.getExitCode()).isEqualTo(1);
    Assertions.assertThat(run.getOut()).contains("Invalid input. Please enter 'yes' or 'no'.");

    run = RunCLI.runWithContinue(registerAllTablesArgs(deleteSourceTables));
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    // should abort
    Assertions.assertThat(run.getOut()).contains("Continuing...");
    Assertions.assertThat(run.getOut()).contains("Summary");
  }

  @Order(6)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDryRun(boolean deleteSourceTables) throws Exception {
    RunCLI run =
        registerTablesCLI(
            deleteSourceTables,
            "--source-catalog-type",
            sourceCatalogType,
            "--source-catalog-properties",
            sourceCatalogProperties,
            "--target-catalog-type",
            targetCatalogType,
            "--target-catalog-properties",
            targetCatalogProperties,
            "--dry-run",
            "--output-dir",
            outputDir.getAbsolutePath());

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    // should not prompt for dry run
    Assertions.assertThat(run.getOut())
        .doesNotContain(
            "Have you read the above warnings and are you sure you want to continue? (yes/no):");
    Assertions.assertThat(run.getOut()).contains("Dry run is completed.");
    String operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: \n"
                    + "- Identified 4 tables for %s by dry-run. "
                    + "These identifiers are also written into dry_run_identifiers.txt. "
                    + "You can use this file with `--identifiers-from-file` option.",
                operation));
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Details: \n" + "- Identified these tables for %s by dry-run:\n", operation));
    Assertions.assertThat(new File(dryRunFile).exists()).isTrue();
    Assertions.assertThat(Files.readAllLines(Paths.get(dryRunFile)))
        .containsExactlyInAnyOrder("foo.tbl1", "foo.tbl2", "bar.tbl3", "bar.tbl4");
  }

  private static String[] registerAllTablesArgs(boolean deleteSourceTables) {
    ArrayList<String> args =
        Lists.newArrayList(
            "--source-catalog-type",
            sourceCatalogType,
            "--source-catalog-properties",
            sourceCatalogProperties,
            "--target-catalog-type",
            targetCatalogType,
            "--target-catalog-properties",
            targetCatalogProperties,
            "--output-dir",
            outputDir.getAbsolutePath());
    if (deleteSourceTables) {
      args.add("--delete-source-tables");
    }
    return args.toArray(new String[0]);
  }

  private static RunCLI registerTablesCLI(boolean deleteSourceTables, String... args)
      throws Exception {
    if (!deleteSourceTables) {
      return RunCLI.runWithContinue(args);
    }
    List<String> argsList = Lists.newArrayList(args);
    argsList.add("--delete-source-tables");
    return RunCLI.runWithContinue(argsList.toArray(new String[0]));
  }
}
