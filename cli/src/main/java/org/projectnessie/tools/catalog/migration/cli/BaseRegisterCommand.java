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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.projectnessie.tools.catalog.migration.api.CatalogMigrationResult;
import org.projectnessie.tools.catalog.migration.api.CatalogMigrator;
import org.projectnessie.tools.catalog.migration.api.CatalogMigratorParams;
import org.projectnessie.tools.catalog.migration.api.ImmutableCatalogMigratorParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

public abstract class BaseRegisterCommand implements Callable<Integer> {

  @CommandLine.ArgGroup(
      exclusive = false,
      multiplicity = "1",
      heading = "source catalog options: %n")
  private SourceCatalogOptions sourceCatalogOptions;

  @CommandLine.ArgGroup(
      exclusive = false,
      multiplicity = "1",
      heading = "target catalog options: %n")
  private TargetCatalogOptions targetCatalogOptions;

  @CommandLine.ArgGroup(heading = "identifier options: %n")
  private IdentifierOptions identifierOptions;

  @CommandLine.Option(
      names = {"--output-dir"},
      required = true,
      description =
          "local output directory path to write CLI output files like `failed_identifiers.txt`, "
              + "`failed_to_delete_at_source.txt`, `dry_run_identifiers.txt`. ")
  private Path outputDirPath;

  @CommandLine.Option(
      names = {"--dry-run"},
      description =
          "optional configuration to simulate the registration without actually registering. Can learn about a list "
              + "of the tables that will be registered by running this.")
  private boolean isDryRun;

  @CommandLine.Option(
      names = {"--disable-prompts"},
      description = "optional configuration to disable warning prompts which needs console input.")
  private boolean disablePrompts;

  private boolean deleteSourceCatalogTables;

  private static final int BATCH_SIZE = 100;
  public static final String FAILED_IDENTIFIERS_FILE = "failed_identifiers.txt";
  public static final String FAILED_TO_DELETE_AT_SOURCE_FILE = "failed_to_delete_at_source.txt";
  public static final String DRY_RUN_FILE = "dry_run_identifiers.txt";

  private final Logger consoleLog = LoggerFactory.getLogger("console-log");

  public BaseRegisterCommand() {}

  protected abstract boolean isDeleteSourceCatalogTables();

  @Override
  public Integer call() {
    List<TableIdentifier> identifiers;
    if (identifierOptions != null) {
      identifiers = identifierOptions.processIdentifiersInput();
    } else {
      identifiers = Collections.emptyList();
    }

    Catalog sourceCatalog = sourceCatalogOptions.build();
    consoleLog.info("Configured source catalog: {}", sourceCatalog.name());

    Catalog targetCatalog = targetCatalogOptions.build();
    consoleLog.info("Configured target catalog: {}", targetCatalog.name());

    if (!canProceed(sourceCatalog)) {
      return 0;
    }

    deleteSourceCatalogTables = isDeleteSourceCatalogTables();
    CatalogMigratorParams params =
        ImmutableCatalogMigratorParams.builder()
            .sourceCatalog(sourceCatalog)
            .targetCatalog(targetCatalog)
            .deleteEntriesFromSourceCatalog(deleteSourceCatalogTables)
            .build();
    CatalogMigrator catalogMigrator = new CatalogMigrator(params);

    String identifierRegEx = identifierOptions != null ? identifierOptions.identifiersRegEx : null;
    if (identifiers.isEmpty()) {
      if (identifierRegEx != null) {
        consoleLog.info(
            "User has not specified the table identifiers."
                + " Selecting all the tables from all the namespaces from the source catalog "
                + "which matches the regex pattern:{}",
            identifierRegEx);
      } else {
        consoleLog.info(
            "User has not specified the table identifiers."
                + " Selecting all the tables from all the namespaces from the source catalog.");
      }
      identifiers = catalogMigrator.getMatchingTableIdentifiers(identifierRegEx);
    }

    String operation = deleteSourceCatalogTables ? "migration" : "registration";
    consoleLog.info("Identified {} tables for {}.", identifiers.size(), operation);

    if (isDryRun) {
      writeToFile(outputDirPath.resolve(DRY_RUN_FILE), identifiers);
      consoleLog.info("Dry run is completed.");
      printDryRunResults(identifiers);
      return 0;
    }

    consoleLog.info("Started {} ...", operation);

    List<List<TableIdentifier>> identifierBatches = Lists.partition(identifiers, BATCH_SIZE);
    int totalIdentifiers = identifiers.size();
    AtomicInteger counter = new AtomicInteger();
    identifierBatches.forEach(
        identifierBatch -> {
          catalogMigrator.registerTables(identifierBatch);
          consoleLog.info(
              "Attempted {} for {} tables out of {} tables.",
              operation,
              counter.addAndGet(identifierBatch.size()),
              totalIdentifiers);
        });

    CatalogMigrationResult result = catalogMigrator.result();
    writeToFile(
        outputDirPath.resolve(FAILED_IDENTIFIERS_FILE), result.failedToRegisterTableIdentifiers());
    writeToFile(
        outputDirPath.resolve(FAILED_TO_DELETE_AT_SOURCE_FILE),
        result.failedToDeleteTableIdentifiers());

    consoleLog.info("Finished {} ...", operation);
    printSummary(result, sourceCatalog.name(), targetCatalog.name());
    printDetails(result);
    return 0;
  }

  private boolean canProceed(Catalog sourceCatalog) {
    if (isDryRun || disablePrompts) {
      return true;
    }
    if (deleteSourceCatalogTables) {
      if (sourceCatalog instanceof HadoopCatalog) {
        consoleLog.warn(
            "Source catalog type is HADOOP and it doesn't support dropping tables just from "
                + "catalog. {}Avoid operating the migrated tables from the source catalog after migration. "
                + "Use the tables from target catalog.",
            System.lineSeparator());
      }
      return PromptUtil.proceedForMigration();
    } else {
      return PromptUtil.proceedForRegistration();
    }
  }

  private void printSummary(
      CatalogMigrationResult result, String sourceCatalogName, String targetCatalogName) {
    consoleLog.info("Summary: ");
    if (!result.registeredTableIdentifiers().isEmpty()) {
      consoleLog.info(
          "Successfully {} {} tables from {} catalog to {} catalog.",
          deleteSourceCatalogTables ? "migrated" : "registered",
          result.registeredTableIdentifiers().size(),
          sourceCatalogName,
          targetCatalogName);
    }
    if (!result.failedToRegisterTableIdentifiers().isEmpty()) {
      consoleLog.info(
          "Failed to {} {} tables from {} catalog to {} catalog. "
              + "Please check the `catalog_migration.log` file for the failure reason. "
              + "Failed identifiers are written into `{}`. "
              + "Retry with that file using `--identifiers-from-file` option "
              + "if the failure is because of network/connection timeouts.",
          deleteSourceCatalogTables ? "migrate" : "register",
          result.failedToRegisterTableIdentifiers().size(),
          sourceCatalogName,
          targetCatalogName,
          FAILED_IDENTIFIERS_FILE);
    }
    if (!result.failedToDeleteTableIdentifiers().isEmpty()) {
      consoleLog.info(
          "Failed to delete {} tables from {} catalog. "
              + "Please check the `catalog_migration.log` file for the reason. "
              + "{}Failed to delete identifiers are written into `{}`.",
          result.failedToDeleteTableIdentifiers().size(),
          sourceCatalogName,
          System.lineSeparator(),
          FAILED_TO_DELETE_AT_SOURCE_FILE);
    }
  }

  private void printDetails(CatalogMigrationResult result) {
    consoleLog.info("Details: ");
    if (!result.registeredTableIdentifiers().isEmpty()) {
      consoleLog.info(
          "Successfully {} these tables:{}{}",
          deleteSourceCatalogTables ? "migrated" : "registered",
          System.lineSeparator(),
          result.registeredTableIdentifiers());
    }

    if (!result.failedToRegisterTableIdentifiers().isEmpty()) {
      consoleLog.info(
          "Failed to {} these tables:{}{}",
          deleteSourceCatalogTables ? "migrate" : "register",
          System.lineSeparator(),
          result.failedToRegisterTableIdentifiers());
    }

    if (!result.failedToDeleteTableIdentifiers().isEmpty()) {
      consoleLog.warn(
          "Failed to delete these tables from source catalog:{}{}",
          System.lineSeparator(),
          result.failedToDeleteTableIdentifiers());
    }
  }

  private void printDryRunResults(List<TableIdentifier> result) {
    consoleLog.info("Summary: ");
    if (result.isEmpty()) {
      consoleLog.info(
          "No tables are identified for {}. Please check logs for more info.",
          deleteSourceCatalogTables ? "migration" : "registration");
      return;
    }
    consoleLog.info(
        "Identified {} tables for {} by dry-run. These identifiers are also written into {}. "
            + "You can use this file with `--identifiers-from-file` option.",
        result.size(),
        deleteSourceCatalogTables ? "migration" : "registration",
        DRY_RUN_FILE);

    consoleLog.info("Details: ");
    consoleLog.info(
        "Identified these tables for {} by dry-run:{}{}",
        deleteSourceCatalogTables ? "migration" : "registration",
        System.lineSeparator(),
        result);
  }

  private static void writeToFile(Path filePath, List<TableIdentifier> identifiers) {
    List<String> identifiersString =
        identifiers.stream().map(TableIdentifier::toString).collect(Collectors.toList());
    try {
      Files.write(filePath, identifiersString);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write the file:" + filePath, e);
    }
  }
}
