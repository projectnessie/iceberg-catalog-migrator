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

import static org.projectnessie.tools.catalog.migration.cli.BaseRegisterCommand.DRY_RUN_FILE;
import static org.projectnessie.tools.catalog.migration.cli.BaseRegisterCommand.FAILED_IDENTIFIERS_FILE;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import nl.altindag.log.LogCaptor;
import nl.altindag.log.model.LogEvent;
import org.apache.iceberg.aws.dynamodb.DynamoDbCatalog;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.dell.ecs.EcsCatalog;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.tools.catalog.migration.api.CatalogMigrator;
import org.projectnessie.tools.catalog.migration.api.test.AbstractTest;

public abstract class AbstractCLIMigrationTest extends AbstractTest {

  protected static @TempDir Path warehouse1;

  protected static @TempDir Path warehouse2;

  protected static @TempDir Path outputDir;

  protected static Path dryRunFile;
  protected static Path failedIdentifiersFile;

  protected static String sourceCatalogProperties;
  protected static String targetCatalogProperties;

  protected static String sourceCatalogType;
  protected static String targetCatalogType;

  @BeforeAll
  protected static void initFilesPaths() {
    dryRunFile = outputDir.resolve(DRY_RUN_FILE);
    failedIdentifiersFile = outputDir.resolve(FAILED_IDENTIFIERS_FILE);
  }

  @BeforeEach
  protected void beforeEach() {
    createTables();
  }

  @AfterEach
  protected void afterEach() throws IOException {
    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    // create table will call refresh internally.
    sourceCatalog.createTable(TableIdentifier.of(Namespace.of("bar"), "tblx"), schema).refresh();
    targetCatalog.createTable(TableIdentifier.of(Namespace.of("bar"), "tblx"), schema).refresh();

    dropTables();
    Files.deleteIfExists(dryRunFile);
    Files.deleteIfExists(failedIdentifiersFile);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegister(boolean deleteSourceTables) throws Exception {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    RunCLI run = runCLI(deleteSourceTables, registerAllTablesArgs());

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut())
        .contains(
            "User has not specified the table identifiers. "
                + "Will be selecting all the tables from all the namespaces from the source catalog.");
    String operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 4 tables for %s.", operation));
    operation = deleteSourceTables ? "migrated" : "registered";
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: %nSuccessfully %s 4 tables from %s catalog to %s catalog.",
                operation, sourceCatalogType, targetCatalogType));
    Assertions.assertThat(run.getOut())
        .contains(String.format("Details: %nSuccessfully %s these tables:%n", operation));

    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    targetCatalog.loadTable(TableIdentifier.parse("foo.tbl1")).refresh();

    Assertions.assertThat(targetCatalog.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(targetCatalog.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl3"), TableIdentifier.parse("bar.tbl4"));

    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    sourceCatalog.tableExists(TableIdentifier.parse("foo.tbl1"));

    if (deleteSourceTables && !(sourceCatalog instanceof HadoopCatalog)) {
      // table should be deleted after migration from source catalog
      Assertions.assertThat(sourceCatalog.listTables(Namespace.of("foo"))).isEmpty();
      Assertions.assertThat(sourceCatalog.listTables(Namespace.of("bar"))).isEmpty();
      return;
    }
    // tables should be present in source catalog.
    Assertions.assertThat(sourceCatalog.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(sourceCatalog.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl3"), TableIdentifier.parse("bar.tbl4"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterSelectedTables(boolean deleteSourceTables) throws Exception {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    // using `--identifiers` option
    RunCLI run =
        runCLI(
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
            outputDir.toAbsolutePath().toString(),
            "--disable-safety-prompts");

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
                "Summary: %nSuccessfully %s 1 tables from %s catalog to %s catalog.",
                operation, sourceCatalogType, targetCatalogType));
    Assertions.assertThat(run.getOut())
        .contains(String.format("Details: %nSuccessfully %s these tables:%n[bar.tbl3]", operation));

    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    targetCatalog.loadTable(TableIdentifier.parse("bar.tbl3")).refresh();

    Assertions.assertThat(targetCatalog.listTables(Namespace.of("foo"))).isEmpty();
    Assertions.assertThat(targetCatalog.listTables(Namespace.of("bar")))
        .containsExactly(TableIdentifier.parse("bar.tbl3"));

    Path identifierFile = outputDir.resolve("ids.txt");

    // using `--identifiers-from-file` option
    Files.write(identifierFile, Collections.singletonList("bar.tbl4"));
    run =
        runCLI(
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
            identifierFile.toAbsolutePath().toString(),
            "--output-dir",
            outputDir.toAbsolutePath().toString(),
            "--disable-safety-prompts");
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
                "Summary: %nSuccessfully %s 1 tables from %s catalog to %s catalog.",
                operation, sourceCatalogType, targetCatalogType));
    Assertions.assertThat(run.getOut())
        .contains(String.format("Details: %nSuccessfully %s these tables:%n", operation));

    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    targetCatalog.loadTable(TableIdentifier.parse("bar.tbl3")).refresh();

    Assertions.assertThat(targetCatalog.listTables(Namespace.of("foo"))).isEmpty();
    Assertions.assertThat(targetCatalog.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl4"), TableIdentifier.parse("bar.tbl3"));

    // using `--identifiers-regex` option which matches all the tables starts with "foo."
    run =
        runCLI(
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
            outputDir.toAbsolutePath().toString(),
            "--disable-safety-prompts");
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut())
        .contains(
            "User has not specified the table identifiers. Will be selecting all the tables from all the namespaces "
                + "from the source catalog which matches the regex pattern:^foo\\..*");
    operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 2 tables for %s.", operation));
    operation = deleteSourceTables ? "migrated" : "registered";
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: %nSuccessfully %s 2 tables from %s catalog to %s catalog.",
                operation, sourceCatalogType, targetCatalogType));
    Assertions.assertThat(run.getOut())
        .contains(String.format("Details: %nSuccessfully %s these tables:%n", operation));

    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    targetCatalog.loadTable(TableIdentifier.parse("bar.tbl3")).refresh();

    Assertions.assertThat(targetCatalog.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(targetCatalog.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl3"), TableIdentifier.parse("bar.tbl4"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterError(boolean deleteSourceTables) throws Exception {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);
    // use invalid namespace which leads to NoSuchTableException
    RunCLI run =
        runCLI(
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
            outputDir.toAbsolutePath().toString(),
            "--disable-safety-prompts");
    Assertions.assertThat(run.getExitCode()).isEqualTo(1);
    String operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 1 tables for %s.", operation));
    operation = deleteSourceTables ? "migrate" : "register";
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: %nFailed to %s 1 tables from %s catalog to %s catalog."
                    + " Please check the `catalog_migration.log`",
                operation, sourceCatalogType, targetCatalogType));
    Assertions.assertThat(run.getOut())
        .contains(String.format("Details: %nFailed to %s these tables:%n[dummy.tbl3]", operation));

    // try to register same table twice which leads to AlreadyExistsException
    runCLI(
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
        outputDir.toAbsolutePath().toString(),
        "--disable-safety-prompts");
    run =
        runCLI(
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
            outputDir.toAbsolutePath().toString(),
            "--disable-safety-prompts");
    Assertions.assertThat(run.getExitCode()).isEqualTo(1);
    operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 1 tables for %s.", operation));
    operation = deleteSourceTables ? "migrate" : "register";
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: %nFailed to %s 1 tables from %s catalog to %s catalog."
                    + " Please check the `catalog_migration.log`",
                operation, sourceCatalogType, targetCatalogType));
    Assertions.assertThat(run.getOut())
        .contains(String.format("Details: %nFailed to %s these tables:%n[foo.tbl2]", operation));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterWithFewFailures(boolean deleteSourceTables) throws Exception {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);
    // register only foo.tbl2
    RunCLI run =
        runCLI(
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
            outputDir.toAbsolutePath().toString(),
            "--disable-safety-prompts");
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    String operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 1 tables for %s.", operation));
    operation = deleteSourceTables ? "migrated" : "registered";
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: %nSuccessfully %s 1 tables from %s catalog to %s catalog.",
                operation, sourceCatalogType, targetCatalogType));
    Assertions.assertThat(run.getOut())
        .contains(String.format("Details: %nSuccessfully %s these tables:%n[foo.tbl2]", operation));

    if (deleteSourceTables && !(sourceCatalog instanceof HadoopCatalog)) {
      // create a table with the same name in source catalog which got deleted.
      sourceCatalog.createTable(TableIdentifier.of(Namespace.of("foo"), "tbl2"), schema);
    }

    // register all the tables from source catalog again. So that registering `foo.tbl2` will fail.
    run =
        runCLI(
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
            outputDir.toAbsolutePath().toString(),
            "--disable-safety-prompts");
    Assertions.assertThat(run.getExitCode()).isEqualTo(1);
    operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 4 tables for %s.", operation));
    operation = deleteSourceTables ? "migrated" : "registered";
    String ops = deleteSourceTables ? "migrate" : "register";
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: %n"
                    + "Successfully %s 3 tables from %s catalog to %s catalog.%n"
                    + "Failed to %s 1 tables from %s catalog to %s catalog. "
                    + "Please check the `catalog_migration.log` file for the failure reason. "
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
        .contains(String.format("Details: %nSuccessfully %s these tables:%n", operation));
    Assertions.assertThat(run.getOut())
        .contains(String.format("Failed to %s these tables:%n[foo.tbl2]", ops));

    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    targetCatalog.loadTable(TableIdentifier.parse("bar.tbl3")).refresh();

    Assertions.assertThat(targetCatalog.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(targetCatalog.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl3"), TableIdentifier.parse("bar.tbl4"));

    // retry the failed tables using `--identifiers-from-file`
    run =
        runCLI(
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
            failedIdentifiersFile.toAbsolutePath().toString(),
            "--output-dir",
            outputDir.toAbsolutePath().toString(),
            "--disable-safety-prompts");
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: %n"
                    + "Failed to %s 1 tables from %s catalog to %s catalog. "
                    + "Please check the `catalog_migration.log` file for the failure reason. "
                    + "Failed identifiers are written into `failed_identifiers.txt`. "
                    + "Retry with that file using `--identifiers-from-file` option "
                    + "if the failure is because of network/connection timeouts.",
                ops, sourceCatalogType, targetCatalogType));
    Assertions.assertThat(run.getOut())
        .contains(String.format("Details: %nFailed to %s these tables:%n[foo.tbl2]", ops));
    Assertions.assertThat(Files.exists(failedIdentifiersFile)).isTrue();
    Assertions.assertThat(Files.readAllLines(failedIdentifiersFile)).containsExactly("foo.tbl2");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterNoTables(boolean deleteSourceTables) throws Exception {
    // use source catalog as targetCatalog which has no tables.
    Assumptions.assumeFalse(
        deleteSourceTables && targetCatalog instanceof HadoopCatalog,
        "deleting source tables is unsupported for HadoopCatalog");
    RunCLI run =
        runCLI(
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
            outputDir.toAbsolutePath().toString(),
            "--disable-safety-prompts");

    Assertions.assertThat(run.getExitCode()).isEqualTo(2);
    String operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "No tables were identified for %s. Please check `catalog_migration.log` file for more info.",
                operation));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDryRun(boolean deleteSourceTables) throws Exception {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);
    RunCLI run =
        runCLI(
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
            outputDir.toAbsolutePath().toString(),
            "--disable-safety-prompts");

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    // should not prompt for dry run
    Assertions.assertThat(run.getOut())
        .doesNotContain(
            "Are you certain that you wish to proceed, after reading the above warnings? (yes/no):");
    Assertions.assertThat(run.getOut()).contains("Dry run is completed.");
    String operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: %n"
                    + "Identified 4 tables for %s by dry-run. "
                    + "These identifiers are also written into dry_run_identifiers.txt. "
                    + "This file can be used with `--identifiers-from-file` option for an actual run.",
                operation));
    Assertions.assertThat(run.getOut())
        .contains(
            String.format("Details: %nIdentified these tables for %s by dry-run:%n", operation));
    Assertions.assertThat(Files.exists(dryRunFile)).isTrue();
    Assertions.assertThat(Files.readAllLines(dryRunFile))
        .containsExactlyInAnyOrder("foo.tbl1", "foo.tbl2", "bar.tbl3", "bar.tbl4");
  }

  @ParameterizedTest
  @CsvSource(value = {"false,false", "false,true", "true,false", "true,true"})
  public void testStacktrace(boolean deleteSourceTables, boolean enableStacktrace)
      throws Exception {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);
    try (LogCaptor logCaptor = LogCaptor.forClass(CatalogMigrator.class)) {
      runCLI(
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
          "db.dummy_table",
          "--output-dir",
          outputDir.toAbsolutePath().toString(),
          "--disable-safety-prompts",
          "--stacktrace=" + enableStacktrace);

      Assertions.assertThat(logCaptor.getLogEvents()).hasSize(1);
      LogEvent logEvent = logCaptor.getLogEvents().get(0);
      if (enableStacktrace) {
        Assertions.assertThat(logEvent.getFormattedMessage())
            .isEqualTo("Unable to register the table db.dummy_table");
        Assertions.assertThat(logEvent.getThrowable())
            .isPresent()
            .get()
            .isInstanceOf(NoSuchTableException.class);
      } else {
        Assertions.assertThat(logEvent.getFormattedMessage())
            .isEqualTo(
                "Unable to register the table db.dummy_table : Table does not exist: db.dummy_table");
        Assertions.assertThat(logEvent.getThrowable()).isEmpty();
      }
    }
  }

  protected static String[] registerAllTablesArgs() {
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
            outputDir.toAbsolutePath().toString(),
            "--disable-safety-prompts");
    return args.toArray(new String[0]);
  }

  protected static RunCLI runCLI(boolean deleteSourceTables, String... args) throws Exception {
    List<String> argsList = Lists.newArrayList(args);
    if (!deleteSourceTables) {
      argsList.add(0, "register");
    } else {
      argsList.add(0, "migrate");
    }
    return RunCLI.run(argsList.toArray(new String[0]));
  }

  protected static String catalogType(Catalog catalog) {
    if (catalog instanceof DynamoDbCatalog) {
      return CatalogMigrationUtil.CatalogType.DYNAMODB.name();
    } else if (catalog instanceof EcsCatalog) {
      return CatalogMigrationUtil.CatalogType.ECS.name();
    } else if (catalog instanceof GlueCatalog) {
      return CatalogMigrationUtil.CatalogType.GLUE.name();
    } else if (catalog instanceof HadoopCatalog) {
      return CatalogMigrationUtil.CatalogType.HADOOP.name();
    } else if (catalog instanceof HiveCatalog) {
      return CatalogMigrationUtil.CatalogType.HIVE.name();
    } else if (catalog instanceof JdbcCatalog) {
      return CatalogMigrationUtil.CatalogType.JDBC.name();
    } else if (catalog instanceof NessieCatalog) {
      return CatalogMigrationUtil.CatalogType.NESSIE.name();
    } else if (catalog instanceof RESTCatalog) {
      return CatalogMigrationUtil.CatalogType.REST.name();
    } else {
      return CatalogMigrationUtil.CatalogType.CUSTOM.name();
    }
  }
}
