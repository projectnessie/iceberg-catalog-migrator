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
package org.projectnessie.tools.catalog.migration.api;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.stream.IntStream;
import nl.altindag.log.LogCaptor;
import nl.altindag.log.model.LogEvent;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.tools.catalog.migration.api.test.AbstractTest;

public abstract class AbstractTestCatalogMigrator extends AbstractTest {

  protected static @TempDir Path warehouse1;

  protected static @TempDir Path warehouse2;

  @BeforeEach
  protected void beforeEach() {
    createTables();
  }

  @AfterEach
  protected void afterEach() {
    dropTables();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegister(boolean deleteSourceTables) {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    CatalogMigrationResult result = registerAllTables(deleteSourceTables);

    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"),
            TableIdentifier.parse("foo.tbl2"),
            TableIdentifier.parse("bar.tbl3"),
            TableIdentifier.parse("bar.tbl4"));
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    Assertions.assertThat(targetCatalog.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(targetCatalog.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl3"), TableIdentifier.parse("bar.tbl4"));

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
  public void testRegisterSelectedTables(boolean deleteSourceTables) {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    // using `--identifiers` option
    CatalogMigrationResult result =
        catalogMigratorWithDefaultArgs(deleteSourceTables)
            .registerTables(Collections.singletonList(TableIdentifier.parse("bar.tbl3")))
            .result();
    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactly(TableIdentifier.parse("bar.tbl3"));
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    Assertions.assertThat(targetCatalog.listTables(Namespace.of("foo"))).isEmpty();
    Assertions.assertThat(targetCatalog.listTables(Namespace.of("bar")))
        .containsExactly(TableIdentifier.parse("bar.tbl3"));

    // using --identifiers-regex option which matches all the tables starts with "foo."
    CatalogMigrator catalogMigrator = catalogMigratorWithDefaultArgs(deleteSourceTables);
    result =
        catalogMigrator
            .registerTables(catalogMigrator.getMatchingTableIdentifiers("^foo\\..*"))
            .result();
    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    Assertions.assertThat(targetCatalog.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(targetCatalog.listTables(Namespace.of("bar")))
        .containsExactly(TableIdentifier.parse("bar.tbl3"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterError(boolean deleteSourceTables) {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    // use invalid namespace which leads to NoSuchTableException
    CatalogMigrationResult result =
        catalogMigratorWithDefaultArgs(deleteSourceTables)
            .registerTables(Collections.singletonList(TableIdentifier.parse("dummy.tbl3")))
            .result();
    Assertions.assertThat(result.registeredTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToRegisterTableIdentifiers())
        .containsExactly(TableIdentifier.parse("dummy.tbl3"));
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    // try to register same table twice which leads to AlreadyExistsException
    result =
        catalogMigratorWithDefaultArgs(deleteSourceTables)
            .registerTables(Collections.singletonList(TableIdentifier.parse("foo.tbl2")))
            .result();
    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactly(TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    result =
        catalogMigratorWithDefaultArgs(deleteSourceTables)
            .registerTables(Collections.singletonList(TableIdentifier.parse("foo.tbl2")))
            .result();
    Assertions.assertThat(result.registeredTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToRegisterTableIdentifiers())
        .contains(TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterWithFewFailures(boolean deleteSourceTables) {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    // register only foo.tbl2
    CatalogMigrationResult result =
        catalogMigratorWithDefaultArgs(deleteSourceTables)
            .registerTables(Collections.singletonList(TableIdentifier.parse("foo.tbl2")))
            .result();
    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactly(TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    if (deleteSourceTables && !(sourceCatalog instanceof HadoopCatalog)) {
      // create a table with the same name in source catalog which got deleted.
      sourceCatalog.createTable(TableIdentifier.of(Namespace.of("foo"), "tbl2"), schema);
    }

    // register all the tables from source catalog again. So that `foo.tbl2` will fail to register.
    result = registerAllTables(deleteSourceTables);
    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"),
            TableIdentifier.parse("bar.tbl3"),
            TableIdentifier.parse("bar.tbl4"));
    Assertions.assertThat(result.failedToRegisterTableIdentifiers())
        .contains(TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    Assertions.assertThat(targetCatalog.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(targetCatalog.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl3"), TableIdentifier.parse("bar.tbl4"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterNoTables(boolean deleteSourceTables) {
    // use source catalog as targetCatalog which has no tables.
    Assumptions.assumeFalse(
        deleteSourceTables && targetCatalog instanceof HadoopCatalog,
        "deleting source tables is unsupported for HadoopCatalog");
    CatalogMigrator catalogMigrator =
        ImmutableCatalogMigrator.builder()
            .sourceCatalog(targetCatalog)
            .targetCatalog(sourceCatalog)
            .deleteEntriesFromSourceCatalog(deleteSourceTables)
            .build();
    Set<TableIdentifier> matchingTableIdentifiers =
        catalogMigrator.getMatchingTableIdentifiers(null);
    Assertions.assertThat(matchingTableIdentifiers).isEmpty();
    CatalogMigrationResult result =
        catalogMigrator.registerTables(matchingTableIdentifiers).result();
    Assertions.assertThat(result.registeredTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterLargeNumberOfTables(boolean deleteSourceTables) {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    // additionally create 240 tables along with 4 tables created in beforeEach()
    IntStream.range(0, 240)
        .forEach(
            val ->
                sourceCatalog.createTable(
                    TableIdentifier.of(Namespace.of("foo"), "tblx" + val), schema));

    CatalogMigrationResult result;
    result = registerAllTables(deleteSourceTables);

    Assertions.assertThat(result.registeredTableIdentifiers()).hasSize(244);
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    Assertions.assertThat(targetCatalog.listTables(Namespace.of("foo"))).hasSize(242);
    Assertions.assertThat(targetCatalog.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl3"), TableIdentifier.parse("bar.tbl4"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testListingTableIdentifiers(boolean deleteSourceTables) {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    CatalogMigrator catalogMigrator = catalogMigratorWithDefaultArgs(deleteSourceTables);

    // should list all the tables from all the namespace when regex is null.
    Set<TableIdentifier> matchingTableIdentifiers =
        catalogMigrator.getMatchingTableIdentifiers(null);
    Assertions.assertThat(matchingTableIdentifiers)
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"),
            TableIdentifier.parse("foo.tbl2"),
            TableIdentifier.parse("bar.tbl3"),
            TableIdentifier.parse("bar.tbl4"));

    // list the tables whose identifier starts with "foo."
    matchingTableIdentifiers = catalogMigrator.getMatchingTableIdentifiers("^foo\\..*");
    Assertions.assertThat(matchingTableIdentifiers)
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));

    // test filter that doesn't match any table.
    matchingTableIdentifiers = catalogMigrator.getMatchingTableIdentifiers("^dev\\..*");
    Assertions.assertThat(matchingTableIdentifiers).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterWithNewNamespace(boolean deleteSourceTables) {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    // create namespace "db1" only in source catalog
    sourceCatalog.createTable(TableIdentifier.of(Namespace.of("db1"), "tbl5"), schema);

    CatalogMigrationResult result =
        catalogMigratorWithDefaultArgs(deleteSourceTables)
            .registerTables(Collections.singletonList(TableIdentifier.parse("db1.tbl5")))
            .result();

    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactly(TableIdentifier.parse("db1.tbl5"));
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    Assertions.assertThat(targetCatalog.listTables(Namespace.of("db1")))
        .containsExactly(TableIdentifier.parse("db1.tbl5"));
  }

  @ParameterizedTest
  @CsvSource(value = {"false,false", "false,true", "true,false", "true,true"})
  public void testStacktrace(boolean deleteSourceTables, boolean enableStacktrace) {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    ImmutableCatalogMigrator migrator =
        ImmutableCatalogMigrator.builder()
            .sourceCatalog(sourceCatalog)
            .targetCatalog(targetCatalog)
            .deleteEntriesFromSourceCatalog(deleteSourceTables)
            .enableStacktrace(enableStacktrace)
            .build();
    try (LogCaptor logCaptor = LogCaptor.forClass(CatalogMigrator.class)) {
      CatalogMigrationResult result =
          migrator
              .registerTables(Collections.singletonList(TableIdentifier.parse("db.dummy_table")))
              .result();
      Assertions.assertThat(result.registeredTableIdentifiers()).isEmpty();
      Assertions.assertThat(result.failedToRegisterTableIdentifiers())
          .containsExactly(TableIdentifier.parse("db.dummy_table"));
      Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

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

  protected CatalogMigrator catalogMigratorWithDefaultArgs(boolean deleteSourceTables) {
    return ImmutableCatalogMigrator.builder()
        .sourceCatalog(sourceCatalog)
        .targetCatalog(targetCatalog)
        .deleteEntriesFromSourceCatalog(deleteSourceTables)
        .build();
  }

  private CatalogMigrationResult registerAllTables(boolean deleteSourceTables) {
    CatalogMigrator catalogMigrator = catalogMigratorWithDefaultArgs(deleteSourceTables);
    return catalogMigrator
        .registerTables(catalogMigrator.getMatchingTableIdentifiers(null))
        .result();
  }
}
