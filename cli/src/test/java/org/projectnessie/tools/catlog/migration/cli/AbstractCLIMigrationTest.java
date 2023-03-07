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
package org.projectnessie.tools.catlog.migration.cli;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.iceberg.aws.dynamodb.DynamoDbCatalog;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.dell.ecs.EcsCatalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.tools.catalog.migration.api.test.AbstractTest;
import org.projectnessie.tools.catalog.migration.cli.CatalogUtil;

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

  @BeforeEach
  protected void beforeEach() {
    createTables();
  }

  @AfterEach
  protected void afterEach() throws IOException {
    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    // create table will call refresh internally.
    catalog1.createTable(TableIdentifier.of(Namespace.of("bar"), "tblx"), schema).refresh();
    catalog2.createTable(TableIdentifier.of(Namespace.of("bar"), "tblx"), schema).refresh();

    dropTables();
    Files.deleteIfExists(dryRunFile);
    Files.deleteIfExists(failedIdentifiersFile);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegister(boolean deleteSourceTables) throws Exception {
    RunCLI run = runCLI(deleteSourceTables, registerAllTablesArgs());

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
                "Summary: %nSuccessfully %s 4 tables from %s catalog to %s catalog.",
                operation, sourceCatalogType, targetCatalogType));
    Assertions.assertThat(run.getOut())
        .contains(String.format("Details: %nSuccessfully %s these tables:%n", operation));

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
            "--disable-prompts");

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
    catalog2.loadTable(TableIdentifier.parse("bar.tbl3")).refresh();

    Assertions.assertThat(catalog2.listTables(Namespace.of("foo"))).isEmpty();
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar")))
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
            "--disable-prompts");
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
    catalog2.loadTable(TableIdentifier.parse("bar.tbl3")).refresh();

    Assertions.assertThat(catalog2.listTables(Namespace.of("foo"))).isEmpty();
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl4"), TableIdentifier.parse("bar.tbl3"));

    // using --identifiers-regex option which matches all the tables starts with "foo."
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
            "--disable-prompts");
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut())
        .contains(
            "User has not specified the table identifiers. Selecting all the tables from all the namespaces "
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
            "--disable-prompts");
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
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
        "--disable-prompts");
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
            "--disable-prompts");
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
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

  @Order(3)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterWithFewFailures(boolean deleteSourceTables) throws Exception {
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
            "--disable-prompts");
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

    if (deleteSourceTables && !(catalog1 instanceof HadoopCatalog)) {
      // create a table with the same name in source catalog which got deleted.
      catalog1.createTable(TableIdentifier.of(Namespace.of("foo"), "tbl2"), schema);
    }

    // register all the tables from source catalog again
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
            "--disable-prompts");
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
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
    catalog2.loadTable(TableIdentifier.parse("bar.tbl3")).refresh();

    Assertions.assertThat(catalog2.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl3"), TableIdentifier.parse("bar.tbl4"));

    // retry the failed tables using --identifiers-from-file
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
            "--disable-prompts");
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

  @Order(4)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterNoTables(boolean deleteSourceTables) throws Exception {
    // source catalog is catalog2 which has no tables.
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
            "--disable-prompts");

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    String operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 0 tables for %s.", operation));
  }

  @Order(5)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDryRun(boolean deleteSourceTables) throws Exception {
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
            "--disable-prompts");

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
                "Summary: %n"
                    + "Identified 4 tables for %s by dry-run. "
                    + "These identifiers are also written into dry_run_identifiers.txt. "
                    + "You can use this file with `--identifiers-from-file` option.",
                operation));
    Assertions.assertThat(run.getOut())
        .contains(
            String.format("Details: %nIdentified these tables for %s by dry-run:%n", operation));
    Assertions.assertThat(Files.exists(dryRunFile)).isTrue();
    Assertions.assertThat(Files.readAllLines(dryRunFile))
        .containsExactlyInAnyOrder("foo.tbl1", "foo.tbl2", "bar.tbl3", "bar.tbl4");
  }

  @Order(6)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterLargeNumberOfTables(boolean deleteSourceTables) throws Exception {
    // additionally create 240 tables along with 4 tables created in beforeEach()
    IntStream.range(0, 240)
        .forEach(
            val ->
                catalog1.createTable(
                    TableIdentifier.of(Namespace.of("foo"), "tblx" + val), schema));

    RunCLI run = runCLI(deleteSourceTables, registerAllTablesArgs());

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    String operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 244 tables for %s.", operation));
    operation = deleteSourceTables ? "migrated" : "registered";
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: %nSuccessfully %s 244 tables from %s catalog to" + " %s catalog.",
                operation, sourceCatalogType, targetCatalogType));
    Assertions.assertThat(run.getOut())
        .contains(String.format("Details: %nSuccessfully %s these tables:%n", operation));

    operation = deleteSourceTables ? "migration" : "registration";
    // validate intermediate output
    Assertions.assertThat(run.getOut())
        .contains(String.format("Attempted %s for 100 tables out of 244 tables.", operation));
    Assertions.assertThat(run.getOut())
        .contains(String.format("Attempted %s for 200 tables out of 244 tables.", operation));

    Assertions.assertThat(catalog2.listTables(Namespace.of("foo"))).hasSize(242);
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl3"), TableIdentifier.parse("bar.tbl4"));
  }

  private static String[] registerAllTablesArgs() {
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
            "--disable-prompts");
    return args.toArray(new String[0]);
  }

  private static RunCLI runCLI(boolean deleteSourceTables, String... args) throws Exception {
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
      return CatalogUtil.CatalogType.DYNAMODB.name();
    } else if (catalog instanceof EcsCatalog) {
      return CatalogUtil.CatalogType.ECS.name();
    } else if (catalog instanceof GlueCatalog) {
      return CatalogUtil.CatalogType.GLUE.name();
    } else if (catalog instanceof HadoopCatalog) {
      return CatalogUtil.CatalogType.HADOOP.name();
    } else if (catalog instanceof HiveCatalog) {
      return CatalogUtil.CatalogType.HIVE.name();
    } else if (catalog instanceof JdbcCatalog) {
      return CatalogUtil.CatalogType.JDBC.name();
    } else if (catalog instanceof NessieCatalog) {
      return CatalogUtil.CatalogType.NESSIE.name();
    } else if (catalog instanceof RESTCatalog) {
      return CatalogUtil.CatalogType.REST.name();
    } else {
      return CatalogUtil.CatalogType.CUSTOM.name();
    }
  }
}
