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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import nl.altindag.log.LogCaptor;
import nl.altindag.log.model.LogEvent;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.tools.catalog.migration.api.CatalogMigrationUtil;
import org.projectnessie.tools.catalog.migration.api.CatalogMigrator;
import org.projectnessie.tools.catalog.migration.api.test.AbstractTest;

public abstract class AbstractCLIMigrationTest extends AbstractTest {

  protected static @TempDir Path outputDir;

  protected static String sourceCatalogProperties;
  protected static String targetCatalogProperties;

  protected static String sourceCatalogType;
  protected static String targetCatalogType;

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
    String propertiesStr = Joiner.on(",").withKeyValueSeparator("=").join(properties);
    if (isSourceCatalog) {
      sourceCatalog = catalog;
      sourceCatalogProperties = propertiesStr;
      sourceCatalogType = catalogType.name();
    } else {
      targetCatalog = catalog;
      targetCatalogProperties = propertiesStr;
      targetCatalogType = catalogType.name();
    }
  }

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
    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    // create table will call refresh internally.
    sourceCatalog.createTable(TableIdentifier.of(BAR, "tblx"), schema).refresh();
    targetCatalog.createTable(TableIdentifier.of(BAR, "tblx"), schema).refresh();

    dropTables();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegister(boolean deleteSourceTables) throws Exception {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    String operation = deleteSourceTables ? "migration" : "registration";
    String operated = deleteSourceTables ? "migrated" : "registered";

    // register or migrate  all the tables
    RunCLI run = runCLI(deleteSourceTables, defaultArgs());

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut())
        .contains(
            "User has not specified the table identifiers. "
                + "Will be selecting all the tables from all the namespaces from the source catalog.")
        .contains(String.format("Identified 4 tables for %s.", operation))
        .contains(
            String.format(
                "Summary: %nSuccessfully %s 4 tables from %s catalog to %s catalog.",
                operated, sourceCatalogType, targetCatalogType))
        .contains(String.format("Details: %nSuccessfully %s these tables:%n", operated));

    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    targetCatalog.loadTable(FOO_TBL1).refresh();

    Assertions.assertThat(targetCatalog.listTables(FOO))
        .containsExactlyInAnyOrder(FOO_TBL1, FOO_TBL2);
    Assertions.assertThat(targetCatalog.listTables(BAR))
        .containsExactlyInAnyOrder(BAR_TBL3, BAR_TBL4);

    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    sourceCatalog.tableExists(FOO_TBL1);

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
  public void testRegisterSelectedTables(boolean deleteSourceTables) throws Exception {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    String operation = deleteSourceTables ? "migration" : "registration";
    String operated = deleteSourceTables ? "migrated" : "registered";

    // using `--identifiers` option
    List<String> argsList = defaultArgs();
    argsList.addAll(Arrays.asList("--identifiers", "bar.tbl3"));
    RunCLI run = runCLI(deleteSourceTables, argsList);

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut())
        .doesNotContain(
            "User has not specified the table identifiers. "
                + "Selecting all the tables from all the namespaces from the source catalog.")
        .contains(String.format("Identified 1 tables for %s.", operation))
        .contains(
            String.format(
                "Summary: %nSuccessfully %s 1 tables from %s catalog to %s catalog.",
                operated, sourceCatalogType, targetCatalogType))
        .contains(String.format("Details: %nSuccessfully %s these tables:%n[bar.tbl3]", operated));

    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    targetCatalog.loadTable(BAR_TBL3).refresh();

    Assertions.assertThat(targetCatalog.listTables(FOO)).isEmpty();
    Assertions.assertThat(targetCatalog.listTables(BAR)).containsExactly(BAR_TBL3);

    Path identifierFile = outputDir.resolve("ids.txt");

    // using `--identifiers-from-file` option
    Files.write(identifierFile, Collections.singletonList("bar.tbl4"));
    argsList = defaultArgs();
    argsList.addAll(
        Arrays.asList("--identifiers-from-file", identifierFile.toAbsolutePath().toString()));
    run = runCLI(deleteSourceTables, argsList);

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut())
        .doesNotContain(
            "User has not specified the table identifiers. "
                + "Selecting all the tables from all the namespaces from the source catalog.")
        .contains(String.format("Identified 1 tables for %s.", operation))
        .contains(
            String.format(
                "Summary: %nSuccessfully %s 1 tables from %s catalog to %s catalog.",
                operated, sourceCatalogType, targetCatalogType))
        .contains(String.format("Details: %nSuccessfully %s these tables:%n", operated));

    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    targetCatalog.loadTable(BAR_TBL3).refresh();

    Assertions.assertThat(targetCatalog.listTables(FOO)).isEmpty();
    Assertions.assertThat(targetCatalog.listTables(BAR))
        .containsExactlyInAnyOrder(BAR_TBL4, BAR_TBL3);
    Files.delete(identifierFile);

    // using `--identifiers-regex` option which matches all the tables starts with "foo."
    argsList = defaultArgs();
    argsList.addAll(Arrays.asList("--identifiers-regex", "^foo\\..*"));
    run = runCLI(deleteSourceTables, argsList);

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut())
        .contains(
            "User has not specified the table identifiers. Will be selecting all the tables from all the namespaces "
                + "from the source catalog which matches the regex pattern:^foo\\..*")
        .contains(String.format("Identified 2 tables for %s.", operation))
        .contains(
            String.format(
                "Summary: %nSuccessfully %s 2 tables from %s catalog to %s catalog.",
                operated, sourceCatalogType, targetCatalogType))
        .contains(String.format("Details: %nSuccessfully %s these tables:%n", operated));

    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    targetCatalog.loadTable(BAR_TBL3).refresh();

    Assertions.assertThat(targetCatalog.listTables(FOO))
        .containsExactlyInAnyOrder(FOO_TBL1, FOO_TBL2);
    Assertions.assertThat(targetCatalog.listTables(BAR))
        .containsExactlyInAnyOrder(BAR_TBL3, BAR_TBL4);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterError(boolean deleteSourceTables) throws Exception {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    String operation = deleteSourceTables ? "migration" : "registration";
    String operate = deleteSourceTables ? "migrate" : "register";

    // use invalid namespace which leads to NoSuchTableException
    List<String> argsList = defaultArgs();
    argsList.addAll(Arrays.asList("--identifiers", "dummy.tbl3"));
    RunCLI run = runCLI(deleteSourceTables, argsList);

    Assertions.assertThat(run.getExitCode()).isEqualTo(1);
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 1 tables for %s.", operation))
        .contains(
            String.format(
                "Summary: %nFailed to %s 1 tables from %s catalog to %s catalog."
                    + " Please check the `catalog_migration.log`",
                operate, sourceCatalogType, targetCatalogType))
        .contains(String.format("Details: %nFailed to %s these tables:%n[dummy.tbl3]", operate));

    // try to register same table twice which leads to AlreadyExistsException
    argsList = defaultArgs();
    argsList.addAll(Arrays.asList("--identifiers", "foo.tbl2"));
    runCLI(deleteSourceTables, argsList);
    run = RunCLI.run(argsList.toArray(new String[0]));

    Assertions.assertThat(run.getExitCode()).isEqualTo(1);
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 1 tables for %s.", operation))
        .contains(
            String.format(
                "Summary: %nFailed to %s 1 tables from %s catalog to %s catalog."
                    + " Please check the `catalog_migration.log`",
                operate, sourceCatalogType, targetCatalogType))
        .contains(String.format("Details: %nFailed to %s these tables:%n[foo.tbl2]", operate));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterWithFewFailures(boolean deleteSourceTables) throws Exception {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    String operation = deleteSourceTables ? "migration" : "registration";
    String operated = deleteSourceTables ? "migrated" : "registered";
    String operate = deleteSourceTables ? "migrate" : "register";

    // register only foo.tbl2
    List<String> argsList = defaultArgs();
    argsList.addAll(Arrays.asList("--identifiers", "foo.tbl2"));
    RunCLI run = runCLI(deleteSourceTables, argsList);

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 1 tables for %s.", operation))
        .contains(
            String.format(
                "Summary: %nSuccessfully %s 1 tables from %s catalog to %s catalog.",
                operated, sourceCatalogType, targetCatalogType))
        .contains(String.format("Details: %nSuccessfully %s these tables:%n[foo.tbl2]", operated));

    if (deleteSourceTables) {
      // create a table with the same name in source catalog which got deleted.
      sourceCatalog.createTable(FOO_TBL2, schema);
    }

    // register all the tables from source catalog again. So that registering `foo.tbl2` will fail.
    run = runCLI(deleteSourceTables, defaultArgs());

    Assertions.assertThat(run.getExitCode()).isEqualTo(1);
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 4 tables for %s.", operation))
        .contains(
            String.format(
                "Summary: %n"
                    + "Successfully %s 3 tables from %s catalog to %s catalog.%n"
                    + "Failed to %s 1 tables from %s catalog to %s catalog. "
                    + "Please check the `catalog_migration.log` file for the failure reason. "
                    + "Failed identifiers are written into `failed_identifiers.txt`. "
                    + "Retry with that file using `--identifiers-from-file` option "
                    + "if the failure is because of network/connection timeouts.",
                operated,
                sourceCatalogType,
                targetCatalogType,
                operate,
                sourceCatalogType,
                targetCatalogType))
        .contains(String.format("Details: %nSuccessfully %s these tables:%n", operated))
        .contains(String.format("Failed to %s these tables:%n[foo.tbl2]", operate));

    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    targetCatalog.loadTable(BAR_TBL3).refresh();

    Assertions.assertThat(targetCatalog.listTables(FOO))
        .containsExactlyInAnyOrder(FOO_TBL1, FOO_TBL2);
    Assertions.assertThat(targetCatalog.listTables(BAR))
        .containsExactlyInAnyOrder(BAR_TBL3, BAR_TBL4);

    Path failedIdentifiersFile = outputDir.resolve(FAILED_IDENTIFIERS_FILE);

    // retry the failed tables using `--identifiers-from-file`
    argsList = defaultArgs();
    argsList.addAll(
        Arrays.asList(
            "--identifiers-from-file", failedIdentifiersFile.toAbsolutePath().toString()));
    run = runCLI(deleteSourceTables, argsList);

    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: %n"
                    + "Failed to %s 1 tables from %s catalog to %s catalog. "
                    + "Please check the `catalog_migration.log` file for the failure reason. "
                    + "Failed identifiers are written into `failed_identifiers.txt`. "
                    + "Retry with that file using `--identifiers-from-file` option "
                    + "if the failure is because of network/connection timeouts.",
                operate, sourceCatalogType, targetCatalogType))
        .contains(String.format("Details: %nFailed to %s these tables:%n[foo.tbl2]", operate));
    Assertions.assertThat(failedIdentifiersFile).exists();
    Assertions.assertThat(Files.readAllLines(failedIdentifiersFile)).containsExactly("foo.tbl2");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterNoTables(boolean deleteSourceTables) throws Exception {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    // clean up the default tables present in the source catalog.
    dropTables();

    RunCLI run = runCLI(deleteSourceTables, defaultArgs());

    Assertions.assertThat(run.getExitCode()).isEqualTo(1);
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

    List<String> argsList = defaultArgs();
    argsList.add("--dry-run");
    RunCLI run = runCLI(deleteSourceTables, argsList);

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    String operation = deleteSourceTables ? "migration" : "registration";
    // should not prompt for dry run
    Assertions.assertThat(run.getOut())
        .doesNotContain(
            "Are you certain that you wish to proceed, after reading the above warnings? (yes/no):")
        .contains("Dry run is completed.")
        .contains(
            String.format(
                "Summary: %n"
                    + "Identified 4 tables for %s by dry-run. "
                    + "These identifiers are also written into dry_run_identifiers.txt. "
                    + "This file can be used with `--identifiers-from-file` option for an actual run.",
                operation))
        .contains(
            String.format("Details: %nIdentified these tables for %s by dry-run:%n", operation));
    Path dryRunFile = outputDir.resolve(DRY_RUN_FILE);
    Assertions.assertThat(dryRunFile).exists();
    Assertions.assertThat(Files.readAllLines(dryRunFile))
        .containsExactlyInAnyOrder("foo.tbl1", "foo.tbl2", "bar.tbl3", "bar.tbl4");
  }

  @ParameterizedTest
  @CsvSource(value = {"false,false", "false,true", "true,false", "true,true"})
  public void testStacktrace(boolean deleteSourceTables, boolean enableStacktrace)
      throws Exception {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);
    try (LogCaptor logCaptor = LogCaptor.forClass(CatalogMigrator.class)) {
      List<String> argsList = defaultArgs();
      argsList.addAll(
          Arrays.asList("--identifiers", "db.dummy_table", "--stacktrace=" + enableStacktrace));
      runCLI(deleteSourceTables, argsList);

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

  protected static List<String> defaultArgs() {
    return Lists.newArrayList(
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
  }

  protected static RunCLI runCLI(boolean deleteSourceTables, List<String> argsList)
      throws Exception {
    if (!deleteSourceTables) {
      argsList.add(0, "register");
    } else {
      argsList.add(0, "migrate");
    }
    return RunCLI.run(argsList.toArray(new String[0]));
  }
}
