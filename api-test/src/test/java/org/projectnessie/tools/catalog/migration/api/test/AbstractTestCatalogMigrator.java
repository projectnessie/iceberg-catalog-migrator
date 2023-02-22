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
package org.projectnessie.tools.catalog.migration.api.test;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.stream.IntStream;
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
import org.projectnessie.tools.catalog.migration.api.CatalogMigrationResult;
import org.projectnessie.tools.catalog.migration.api.CatalogMigrator;
import org.projectnessie.tools.catalog.migration.api.ImmutableCatalogMigratorParams;

public abstract class AbstractTestCatalogMigrator extends AbstractTest {

  protected static @TempDir Path warehouse1;

  protected static @TempDir Path warehouse2;

  protected static @TempDir Path outputDir;

  protected static Path dryRunFile;
  protected static Path failedIdentifiersFile;

  private static StringWriter stringWriter;
  private static PrintWriter printWriter;

  @BeforeEach
  protected void beforeEach() {
    createTables();

    stringWriter = new StringWriter();
    printWriter = new PrintWriter(stringWriter);
  }

  @AfterEach
  protected void afterEach() throws IOException {
    dropTables();
    deleteFileIfExists(dryRunFile);
    deleteFileIfExists(failedIdentifiersFile);
    stringWriter.close();
    printWriter.close();
  }

  @Order(0)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegister(boolean deleteSourceTables) {

    CatalogMigrationResult result;
    result = registerAllTables(deleteSourceTables);

    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"),
            TableIdentifier.parse("foo.tbl2"),
            TableIdentifier.parse("bar.tbl3"),
            TableIdentifier.parse("bar.tbl4"));
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    String output = stringWriter.toString();
    Assertions.assertThat(output)
        .contains(
            "User has not specified the table identifiers. "
                + "Selecting all the tables from all the namespaces from the source catalog.");
    String operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(output).contains(String.format("Identified 4 tables for %s.", operation));
    operation = deleteSourceTables ? "migrated" : "registered";
    Assertions.assertThat(output)
        .contains(
            String.format(
                "Summary: %n- Successfully %s 4 tables from %s catalog to" + " %s catalog.",
                operation, catalog1.name(), catalog2.name()));
    Assertions.assertThat(output)
        .contains(String.format("Details: %n" + "- Successfully %s these tables:%n", operation));

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
  public void testRegisterSelectedTables(boolean deleteSourceTables) {
    // using `--identifiers` option
    ImmutableCatalogMigratorParams.Builder builder = builderWithDefaultArgs(deleteSourceTables);
    builder.tableIdentifiers(Collections.singletonList(TableIdentifier.parse("bar.tbl3")));
    CatalogMigrationResult result = CatalogMigrator.registerTables(builder.build());
    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactly(TableIdentifier.parse("bar.tbl3"));
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    String output = stringWriter.toString();
    Assertions.assertThat(output)
        .doesNotContain(
            "User has not specified the table identifiers. "
                + "Selecting all the tables from all the namespaces from the source catalog.");
    String operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(output).contains(String.format("Identified 1 tables for %s.", operation));
    operation = deleteSourceTables ? "migrated" : "registered";
    Assertions.assertThat(output)
        .contains(
            String.format(
                "Summary: %n- Successfully %s 1 tables from %s catalog to" + " %s catalog.",
                operation, catalog1.name(), catalog2.name()));
    Assertions.assertThat(output)
        .contains(
            String.format(
                "Details: %n- Successfully %s these tables:%n" + "[bar.tbl3]", operation));

    Assertions.assertThat(catalog2.listTables(Namespace.of("foo"))).isEmpty();
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar")))
        .containsExactly(TableIdentifier.parse("bar.tbl3"));

    // using --identifiers-regex option which matches all the tables starts with "foo."
    builder = builderWithDefaultArgs(deleteSourceTables);
    builder.identifierRegex("^foo\\..*");
    result = CatalogMigrator.registerTables(builder.build());
    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    output = stringWriter.toString();
    Assertions.assertThat(output)
        .contains(
            "User has not specified the table identifiers. Selecting all the tables from all the namespaces "
                + "from the source catalog which matches the regex pattern:^foo\\..*");
    Assertions.assertThat(output)
        .contains(
            "Collecting all the tables from all the namespaces of source catalog "
                + "which matches the regex pattern:^foo\\..*");
    operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(output).contains(String.format("Identified 2 tables for %s.", operation));
    operation = deleteSourceTables ? "migrated" : "registered";
    Assertions.assertThat(output)
        .contains(
            String.format(
                "Summary: %n- Successfully %s 2 tables from %s catalog to" + " %s catalog.",
                operation, catalog1.name(), catalog2.name()));
    Assertions.assertThat(output)
        .contains(String.format("Details: %n" + "- Successfully %s these tables:%n", operation));

    Assertions.assertThat(catalog2.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar")))
        .containsExactly(TableIdentifier.parse("bar.tbl3"));
  }

  @Order(2)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterError(boolean deleteSourceTables) {
    // use invalid namespace which leads to NoSuchTableException
    ImmutableCatalogMigratorParams.Builder builder = builderWithDefaultArgs(deleteSourceTables);
    builder.tableIdentifiers(Collections.singletonList(TableIdentifier.parse("dummy.tbl3")));
    CatalogMigrationResult result = CatalogMigrator.registerTables(builder.build());
    Assertions.assertThat(result.registeredTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToRegisterTableIdentifiers())
        .containsExactly(TableIdentifier.parse("dummy.tbl3"));
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    String output = stringWriter.toString();
    String operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(output).contains(String.format("Identified 1 tables for %s.", operation));
    operation = deleteSourceTables ? "migrate" : "register";
    Assertions.assertThat(output)
        .contains(
            String.format(
                "Summary: %n- Failed to %s 1 tables from %s catalog to %s catalog."
                    + " Please check the `catalog_migration.log`",
                operation, catalog1.name(), catalog2.name()));
    Assertions.assertThat(output)
        .contains(
            String.format("Details: %n- Failed to %s these tables:%n[dummy.tbl3]", operation));

    // try to register same table twice which leads to AlreadyExistsException
    builder = builderWithDefaultArgs(deleteSourceTables);
    builder.tableIdentifiers(Collections.singletonList(TableIdentifier.parse("foo.tbl2")));
    result = CatalogMigrator.registerTables(builder.build());
    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactly(TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    builder = builderWithDefaultArgs(deleteSourceTables);
    builder.tableIdentifiers(Collections.singletonList(TableIdentifier.parse("foo.tbl2")));
    result = CatalogMigrator.registerTables(builder.build());
    Assertions.assertThat(result.registeredTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToRegisterTableIdentifiers())
        .contains(TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    output = stringWriter.toString();
    operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(output).contains(String.format("Identified 1 tables for %s.", operation));
    operation = deleteSourceTables ? "migrate" : "register";
    Assertions.assertThat(output)
        .contains(
            String.format(
                "Summary: %n- Failed to %s 1 tables from %s catalog to %s catalog."
                    + " Please check the `catalog_migration.log`",
                operation, catalog1.name(), catalog2.name()));
    Assertions.assertThat(output)
        .contains(String.format("Details: %n- Failed to %s these tables:%n[foo.tbl2]", operation));
  }

  @Order(3)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterWithFewFailures(boolean deleteSourceTables) {
    // register only foo.tbl2
    ImmutableCatalogMigratorParams.Builder builder = builderWithDefaultArgs(deleteSourceTables);
    builder.tableIdentifiers(Collections.singletonList(TableIdentifier.parse("foo.tbl2")));
    CatalogMigrationResult result = CatalogMigrator.registerTables(builder.build());
    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactly(TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();
    String output = stringWriter.toString();
    String operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(output).contains(String.format("Identified 1 tables for %s.", operation));
    operation = deleteSourceTables ? "migrated" : "registered";
    Assertions.assertThat(output)
        .contains(
            String.format(
                "Summary: %n- Successfully %s 1 tables from %s catalog to %s catalog.",
                operation, catalog1.name(), catalog2.name()));
    Assertions.assertThat(output)
        .contains(
            String.format(
                "Details: %n" + "- Successfully %s these tables:%n" + "[foo.tbl2]", operation));

    if (deleteSourceTables && !(catalog1 instanceof HadoopCatalog)) {
      // create a table with the same name in source catalog which got deleted.
      catalog1.createTable(TableIdentifier.of(Namespace.of("foo"), "tbl2"), schema);
    }

    // register all the tables from source catalog again
    result = registerAllTables(deleteSourceTables);
    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"),
            TableIdentifier.parse("bar.tbl3"),
            TableIdentifier.parse("bar.tbl4"));
    Assertions.assertThat(result.failedToRegisterTableIdentifiers())
        .contains(TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    output = stringWriter.toString();
    operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(output).contains(String.format("Identified 4 tables for %s.", operation));
    operation = deleteSourceTables ? "migrated" : "registered";
    String ops = deleteSourceTables ? "migrate" : "register";
    Assertions.assertThat(output)
        .contains(
            String.format(
                "Summary: %n"
                    + "- Successfully %s 3 tables from %s catalog to %s catalog.%n"
                    + "- Failed to %s 1 tables from %s catalog to %s catalog. "
                    + "Please check the `catalog_migration.log` file for the failure reason. %n"
                    + "Failed identifiers are written into `failed_identifiers.txt`. "
                    + "Retry with that file using `--identifiers-from-file` option "
                    + "if the failure is because of network/connection timeouts.",
                operation,
                catalog1.name(),
                catalog2.name(),
                ops,
                catalog1.name(),
                catalog2.name()));
    Assertions.assertThat(output)
        .contains(String.format("Details: %n" + "- Successfully %s these tables:%n", operation));
    Assertions.assertThat(output)
        .contains(String.format("- Failed to %s these tables:%n[foo.tbl2]", ops));
    Assertions.assertThat(catalog2.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl3"), TableIdentifier.parse("bar.tbl4"));
  }

  @Order(4)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterNoTables(boolean deleteSourceTables) {
    // source catalog is catalog2 which has no tables.
    ImmutableCatalogMigratorParams.Builder builder =
        ImmutableCatalogMigratorParams.builder()
            .sourceCatalog(catalog2)
            .targetCatalog(catalog1)
            .isDryRun(false)
            .printWriter(printWriter)
            .outputDirPath(outputDir.toAbsolutePath().toString())
            .deleteEntriesFromSourceCatalog(deleteSourceTables);
    CatalogMigrationResult result = CatalogMigrator.registerTables(builder.build());
    Assertions.assertThat(result.registeredTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    String output = stringWriter.toString();
    String operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(output).contains(String.format("Identified 0 tables for %s.", operation));
  }

  @Order(5)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDryRun(boolean deleteSourceTables) throws Exception {
    ImmutableCatalogMigratorParams.Builder builder = builderWithDefaultArgs(deleteSourceTables);
    builder.isDryRun(true);
    CatalogMigrationResult result = CatalogMigrator.registerTables(builder.build());
    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"),
            TableIdentifier.parse("foo.tbl2"),
            TableIdentifier.parse("bar.tbl3"),
            TableIdentifier.parse("bar.tbl4"));
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    String output = stringWriter.toString();
    // should not prompt for dry run
    Assertions.assertThat(output)
        .doesNotContain(
            "Have you read the above warnings and are you sure you want to continue? (yes/no):");
    Assertions.assertThat(output).contains("Dry run is completed.");
    String operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(output)
        .contains(
            String.format(
                "Summary: %n"
                    + "- Identified 4 tables for %s by dry-run. "
                    + "These identifiers are also written into dry_run_identifiers.txt. "
                    + "You can use this file with `--identifiers-from-file` option.",
                operation));
    Assertions.assertThat(output)
        .contains(
            String.format(
                "Details: %n" + "- Identified these tables for %s by dry-run:%n", operation));
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

    CatalogMigrationResult result;
    result = registerAllTables(deleteSourceTables);

    Assertions.assertThat(result.registeredTableIdentifiers()).hasSize(244);
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    String operation = deleteSourceTables ? "migration" : "registration";
    String output = stringWriter.toString();
    Assertions.assertThat(output)
        .contains(String.format("Identified 244 tables for %s.", operation));
    operation = deleteSourceTables ? "migrated" : "registered";
    Assertions.assertThat(output)
        .contains(
            String.format(
                "Summary: %n- Successfully %s 244 tables from %s catalog to" + " %s catalog.",
                operation, catalog1.name(), catalog2.name()));
    Assertions.assertThat(output)
        .contains(String.format("Details: %n" + "- Successfully %s these tables:%n", operation));

    operation = deleteSourceTables ? "migration" : "registration";
    // validate intermediate output
    Assertions.assertThat(output)
        .contains(String.format("Attempted %s for 100 tables out of 244 tables.", operation));
    Assertions.assertThat(output)
        .contains(String.format("Attempted %s for 200 tables out of 244 tables.", operation));

    Assertions.assertThat(catalog2.listTables(Namespace.of("foo"))).hasSize(242);
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl3"), TableIdentifier.parse("bar.tbl4"));
  }

  private CatalogMigrationResult registerAllTables(boolean deleteSourceTables) {
    ImmutableCatalogMigratorParams.Builder builder = builderWithDefaultArgs(deleteSourceTables);
    return CatalogMigrator.registerTables(builder.build());
  }

  private ImmutableCatalogMigratorParams.Builder builderWithDefaultArgs(
      boolean deleteSourceTables) {
    return ImmutableCatalogMigratorParams.builder()
        .sourceCatalog(catalog1)
        .targetCatalog(catalog2)
        .isDryRun(false)
        .printWriter(printWriter)
        .outputDirPath(outputDir.toAbsolutePath().toString())
        .deleteEntriesFromSourceCatalog(deleteSourceTables);
  }
}
