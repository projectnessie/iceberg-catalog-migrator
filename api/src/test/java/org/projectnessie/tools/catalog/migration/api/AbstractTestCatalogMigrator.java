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
import java.util.List;
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

  @Order(0)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegister(boolean deleteSourceTables) {
    CatalogMigrationResult result = registerAllTables(deleteSourceTables);

    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"),
            TableIdentifier.parse("foo.tbl2"),
            TableIdentifier.parse("bar.tbl3"),
            TableIdentifier.parse("bar.tbl4"));
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    Assertions.assertThat(catalog2.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl3"), TableIdentifier.parse("bar.tbl4"));

    if (deleteSourceTables && !(catalog1 instanceof HadoopCatalog)) {
      // table should be deleted after migration from source catalog
      Assertions.assertThat(catalog1.listTables(Namespace.of("foo"))).isEmpty();
      Assertions.assertThat(catalog1.listTables(Namespace.of("bar"))).isEmpty();
      return;
    }
    // tables should be present in source catalog.
    Assertions.assertThat(catalog1.listTables(Namespace.of("foo")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(catalog1.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl3"), TableIdentifier.parse("bar.tbl4"));
  }

  @Order(1)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterSelectedTables(boolean deleteSourceTables) {
    // using `--identifiers` option
    CatalogMigrationResult result =
        catalogMigratorWithDefaultArgs(deleteSourceTables)
            .registerTables(Collections.singletonList(TableIdentifier.parse("bar.tbl3")))
            .result();
    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactly(TableIdentifier.parse("bar.tbl3"));
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    Assertions.assertThat(catalog2.listTables(Namespace.of("foo"))).isEmpty();
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar")))
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

  @Order(3)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterWithFewFailures(boolean deleteSourceTables) {
    // register only foo.tbl2
    CatalogMigrationResult result =
        catalogMigratorWithDefaultArgs(deleteSourceTables)
            .registerTables(Collections.singletonList(TableIdentifier.parse("foo.tbl2")))
            .result();
    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactly(TableIdentifier.parse("foo.tbl2"));
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

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
    CatalogMigrator catalogMigrator =
        ImmutableCatalogMigrator.builder()
            .sourceCatalog(catalog2)
            .targetCatalog(catalog1)
            .deleteEntriesFromSourceCatalog(deleteSourceTables)
            .build();
    List<TableIdentifier> matchingTableIdentifiers =
        catalogMigrator.getMatchingTableIdentifiers(null);
    Assertions.assertThat(matchingTableIdentifiers).isEmpty();
    CatalogMigrationResult result =
        catalogMigrator.registerTables(matchingTableIdentifiers).result();
    Assertions.assertThat(result.registeredTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();
  }

  @Order(5)
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

    Assertions.assertThat(catalog2.listTables(Namespace.of("foo"))).hasSize(242);
    Assertions.assertThat(catalog2.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl3"), TableIdentifier.parse("bar.tbl4"));
  }

  @Order(6)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testListingTableIdentifiers(boolean deleteSourceTables) {
    CatalogMigrator catalogMigrator = catalogMigratorWithDefaultArgs(deleteSourceTables);

    List<TableIdentifier> matchingTableIdentifiers =
        catalogMigrator.getMatchingTableIdentifiers(null);
    Assertions.assertThat(matchingTableIdentifiers)
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"),
            TableIdentifier.parse("foo.tbl2"),
            TableIdentifier.parse("bar.tbl3"),
            TableIdentifier.parse("bar.tbl4"));

    matchingTableIdentifiers = catalogMigrator.getMatchingTableIdentifiers("^foo\\..*");
    Assertions.assertThat(matchingTableIdentifiers)
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.tbl1"), TableIdentifier.parse("foo.tbl2"));
  }

  @Order(7)
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterWithNewNamespace(boolean deleteSourceTables) {
    // catalog2 doesn't have a namespace "db1"
    catalog1.createTable(TableIdentifier.of(Namespace.of("db1"), "tbl5"), schema);

    CatalogMigrationResult result =
        catalogMigratorWithDefaultArgs(deleteSourceTables)
            .registerTables(Collections.singletonList(TableIdentifier.parse("db1.tbl5")))
            .result();

    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactly(TableIdentifier.parse("db1.tbl5"));
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    Assertions.assertThat(catalog2.listTables(Namespace.of("db1")))
        .containsExactly(TableIdentifier.parse("db1.tbl5"));
  }

  protected CatalogMigrator catalogMigratorWithDefaultArgs(boolean deleteSourceTables) {
    return ImmutableCatalogMigrator.builder()
        .sourceCatalog(catalog1)
        .targetCatalog(catalog2)
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
