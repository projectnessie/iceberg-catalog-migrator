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

import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import nl.altindag.log.LogCaptor;
import nl.altindag.log.model.LogEvent;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.tools.catalog.migration.api.test.AbstractTest;

public abstract class AbstractTestCatalogMigrator extends AbstractTest {

  protected static final Namespace NS1 = Namespace.of("ns1");
  protected static final Namespace NS2 = Namespace.of("ns2");
  protected static final Namespace NS3 = Namespace.of("ns3");
  protected static final Namespace NS1_NS2 = Namespace.of("ns1", "ns2");
  protected static final Namespace NS1_NS3 = Namespace.of("ns1", "ns3");
  protected static final Namespace NS1_NS2_NS3 = Namespace.of("ns1", "ns2", "ns3");

  protected static final TableIdentifier TBL = TableIdentifier.parse("tblz");
  protected static final TableIdentifier NS1_TBL = TableIdentifier.of(NS1, "tblz");
  protected static final TableIdentifier NS2_TBL = TableIdentifier.of(NS2, "tblz");
  protected static final TableIdentifier NS3_TBL = TableIdentifier.of(NS3, "tblz");
  protected static final TableIdentifier NS1_NS2_TBL = TableIdentifier.of(NS1_NS2, "tblz");
  protected static final TableIdentifier NS1_NS3_TBL = TableIdentifier.of(NS1_NS3, "tblz");
  protected static final TableIdentifier NS1_NS2_NS3_TBL = TableIdentifier.of(NS1_NS2_NS3, "tblz");

  @AfterAll
  protected static void tearDown() throws Exception {
    dropNamespaces();
  }

  @BeforeEach
  protected void beforeEach() {
    createTables();
  }

  @AfterEach
  protected void afterEach() {
    dropTables();
  }

  protected static void initializeSourceCatalog(
      CatalogMigrationUtil.CatalogType catalogType, Map<String, String> additionalProp) {
    initializeCatalog(true, catalogType, additionalProp);
    createNamespacesForSourceCatalog();
  }

  protected static void initializeTargetCatalog(
      CatalogMigrationUtil.CatalogType catalogType, Map<String, String> additionalProp) {
    initializeCatalog(false, catalogType, additionalProp);
    createNamespacesForTargetCatalog();
  }

  private static void initializeCatalog(
      boolean isSourceCatalog,
      CatalogMigrationUtil.CatalogType catalogType,
      Map<String, String> additionalProp) {
    Map<String, String> properties;
    switch (catalogType) {
      case HADOOP:
        properties = hadoopCatalogProperties(isSourceCatalog);
        break;
      case NESSIE:
        properties = nessieCatalogProperties(isSourceCatalog);
        break;
      case HIVE:
        properties = hiveCatalogProperties(isSourceCatalog, additionalProp);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported for catalog type: %s", catalogType));
    }
    Catalog catalog =
        CatalogMigrationUtil.buildCatalog(
            properties,
            catalogType,
            isSourceCatalog ? "sourceCatalog" : "targetCatalog" + "_" + catalogType,
            null,
            null);
    if (isSourceCatalog) {
      sourceCatalog = catalog;
    } else {
      targetCatalog = catalog;
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegister(boolean deleteSourceTables) {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    CatalogMigrationResult result = registerAllTables(deleteSourceTables);

    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactlyInAnyOrder(FOO_TBL1, FOO_TBL2, BAR_TBL3, BAR_TBL4);
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    Assertions.assertThat(targetCatalog.listTables(FOO))
        .containsExactlyInAnyOrder(FOO_TBL1, FOO_TBL2);
    Assertions.assertThat(targetCatalog.listTables(BAR))
        .containsExactlyInAnyOrder(BAR_TBL3, BAR_TBL4);

    if (deleteSourceTables) {
      // table should be deleted after migration from source catalog
      Assertions.assertThat(sourceCatalog.listTables(FOO)).isEmpty();
      Assertions.assertThat(sourceCatalog.listTables(BAR)).isEmpty();
    } else {
      // tables should be present in source catalog.
      Assertions.assertThat(sourceCatalog.listTables(FOO))
          .containsExactlyInAnyOrder(FOO_TBL1, FOO_TBL2);
      Assertions.assertThat(sourceCatalog.listTables(BAR))
          .containsExactlyInAnyOrder(BAR_TBL3, BAR_TBL4);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterSelectedTables(boolean deleteSourceTables) {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    // using `--identifiers` option
    CatalogMigrationResult result =
        catalogMigratorWithDefaultArgs(deleteSourceTables).registerTable(BAR_TBL3).result();
    Assertions.assertThat(result.registeredTableIdentifiers()).containsExactly(BAR_TBL3);
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    Assertions.assertThat(targetCatalog.listTables(FOO)).isEmpty();
    Assertions.assertThat(targetCatalog.listTables(BAR)).containsExactly(BAR_TBL3);

    // using --identifiers-regex option which matches all the tables starts with "foo."
    CatalogMigrator catalogMigrator = catalogMigratorWithDefaultArgs(deleteSourceTables);
    result =
        catalogMigrator
            .registerTables(catalogMigrator.getMatchingTableIdentifiers("^foo\\..*"))
            .result();
    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactlyInAnyOrder(FOO_TBL1, FOO_TBL2);
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    Assertions.assertThat(targetCatalog.listTables(FOO))
        .containsExactlyInAnyOrder(FOO_TBL1, FOO_TBL2);
    Assertions.assertThat(targetCatalog.listTables(BAR)).containsExactly(BAR_TBL3);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterError(boolean deleteSourceTables) {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    // use invalid namespace which leads to NoSuchTableException
    TableIdentifier identifier = TableIdentifier.parse("dummy.tbl3");
    CatalogMigrationResult result =
        catalogMigratorWithDefaultArgs(deleteSourceTables).registerTable(identifier).result();
    Assertions.assertThat(result.registeredTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).containsExactly(identifier);
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    // try to register same table twice which leads to AlreadyExistsException
    result = catalogMigratorWithDefaultArgs(deleteSourceTables).registerTable(FOO_TBL2).result();
    Assertions.assertThat(result.registeredTableIdentifiers()).containsExactly(FOO_TBL2);
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    result = catalogMigratorWithDefaultArgs(deleteSourceTables).registerTable(FOO_TBL2).result();
    Assertions.assertThat(result.registeredTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).contains(FOO_TBL2);
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterWithFewFailures(boolean deleteSourceTables) {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    // register only foo.tbl2
    CatalogMigrationResult result =
        catalogMigratorWithDefaultArgs(deleteSourceTables).registerTable(FOO_TBL2).result();
    Assertions.assertThat(result.registeredTableIdentifiers()).containsExactly(FOO_TBL2);
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    if (deleteSourceTables) {
      // create a table with the same name in source catalog which got deleted.
      sourceCatalog.createTable(FOO_TBL2, schema);
    }

    // register all the tables from source catalog again. So that `foo.tbl2` will fail to register.
    result = registerAllTables(deleteSourceTables);
    Assertions.assertThat(result.registeredTableIdentifiers())
        .containsExactlyInAnyOrder(FOO_TBL1, BAR_TBL3, BAR_TBL4);
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).contains(FOO_TBL2);
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    Assertions.assertThat(targetCatalog.listTables(FOO))
        .containsExactlyInAnyOrder(FOO_TBL1, FOO_TBL2);
    Assertions.assertThat(targetCatalog.listTables(BAR))
        .containsExactlyInAnyOrder(BAR_TBL3, BAR_TBL4);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterNoTables(boolean deleteSourceTables) {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    // clean up the default tables present in the source catalog.
    dropTables();

    CatalogMigrator catalogMigrator = catalogMigratorWithDefaultArgs(deleteSourceTables);
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
        .forEach(val -> sourceCatalog.createTable(TableIdentifier.of(FOO, "tblx" + val), schema));

    CatalogMigrationResult result;
    result = registerAllTables(deleteSourceTables);

    Assertions.assertThat(result.registeredTableIdentifiers()).hasSize(244);
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    Assertions.assertThat(targetCatalog.listTables(FOO)).hasSize(242);
    Assertions.assertThat(targetCatalog.listTables(BAR))
        .containsExactlyInAnyOrder(BAR_TBL3, BAR_TBL4);
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
        .containsExactlyInAnyOrder(FOO_TBL1, FOO_TBL2, BAR_TBL3, BAR_TBL4);

    // list the tables whose identifier starts with "foo."
    matchingTableIdentifiers = catalogMigrator.getMatchingTableIdentifiers("^foo\\..*");
    Assertions.assertThat(matchingTableIdentifiers).containsExactlyInAnyOrder(FOO_TBL1, FOO_TBL2);

    // test filter that doesn't match any table.
    matchingTableIdentifiers = catalogMigrator.getMatchingTableIdentifiers("^dev\\..*");
    Assertions.assertThat(matchingTableIdentifiers).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterWithNewNamespace(boolean deleteSourceTables) {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    TableIdentifier tbl5 = TableIdentifier.of(DB1, "tbl5");
    // namespace "db1" exists only in source catalog
    sourceCatalog.createTable(tbl5, schema);

    CatalogMigrationResult result =
        catalogMigratorWithDefaultArgs(deleteSourceTables).registerTable(tbl5).result();

    Assertions.assertThat(result.registeredTableIdentifiers()).containsExactly(tbl5);
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    Assertions.assertThat(targetCatalog.listTables(DB1)).containsExactly(tbl5);
  }

  @ParameterizedTest
  @CsvSource(value = {"false,false", "false,true", "true,false", "true,true"})
  public void testStacktrace(boolean deleteSourceTables, boolean enableStacktrace) {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    TableIdentifier identifier = TableIdentifier.parse("db.dummy_table");

    ImmutableCatalogMigrator migrator =
        ImmutableCatalogMigrator.builder()
            .sourceCatalog(sourceCatalog)
            .targetCatalog(targetCatalog)
            .deleteEntriesFromSourceCatalog(deleteSourceTables)
            .enableStacktrace(enableStacktrace)
            .build();
    try (LogCaptor logCaptor = LogCaptor.forClass(CatalogMigrator.class)) {
      CatalogMigrationResult result = migrator.registerTable(identifier).result();
      Assertions.assertThat(result.registeredTableIdentifiers()).isEmpty();
      Assertions.assertThat(result.failedToRegisterTableIdentifiers()).containsExactly(identifier);
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
