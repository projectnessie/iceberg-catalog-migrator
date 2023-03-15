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

import static org.projectnessie.tools.catalog.migration.cli.PromptUtil.ANSI_YELLOW;

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
import org.projectnessie.tools.catalog.migration.api.CatalogMigrationResult;
import org.projectnessie.tools.catalog.migration.api.CatalogMigrator;
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

  private static final int BATCH_SIZE = 100;
  public static final String FAILED_IDENTIFIERS_FILE = "failed_identifiers.txt";
  public static final String FAILED_TO_DELETE_AT_SOURCE_FILE = "failed_to_delete_at_source.txt";
  public static final String DRY_RUN_FILE = "dry_run_identifiers.txt";

  private final Logger consoleLog = LoggerFactory.getLogger("console-log");

  public BaseRegisterCommand() {}

  protected abstract CatalogMigrator catalogMigrator(Catalog sourceCatalog, Catalog targetCatalog);

  protected abstract boolean canProceed(Catalog sourceCatalog);

  protected abstract String operation();

  protected abstract String operated();

  protected abstract String operate();

  @Override
  public Integer call() {
    List<TableIdentifier> identifiers = Collections.emptyList();
    if (identifierOptions != null) {
      identifiers = identifierOptions.processIdentifiersInput();
    }

    Catalog sourceCatalog = sourceCatalogOptions.build();
    consoleLog.info("Configured source catalog: {}", sourceCatalog.name());

    Catalog targetCatalog = targetCatalogOptions.build();
    consoleLog.info("Configured target catalog: {}", targetCatalog.name());

    if (!isDryRun && !disablePrompts && !canProceed(sourceCatalog)) {
      return 0;
    }

    CatalogMigrator catalogMigrator = catalogMigrator(sourceCatalog, targetCatalog);

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

    consoleLog.info("Identified {} tables for {}.", identifiers.size(), operation());

    if (isDryRun) {
      handleDryRunResult(identifiers);
      return 0;
    }

    consoleLog.info("Started {} ...", operation());

    List<List<TableIdentifier>> identifierBatches = Lists.partition(identifiers, BATCH_SIZE);
    int totalIdentifiers = identifiers.size();
    AtomicInteger counter = new AtomicInteger();
    identifierBatches.forEach(
        identifierBatch -> {
          catalogMigrator.registerTables(identifierBatch);
          consoleLog.info(
              "Attempted {} for {} tables out of {} tables.",
              operation(),
              counter.addAndGet(identifierBatch.size()),
              totalIdentifiers);
        });

    handleResults(catalogMigrator.result());
    return 0;
  }

  private void handleResults(CatalogMigrationResult result) {
    writeToFile(
        outputDirPath.resolve(FAILED_IDENTIFIERS_FILE), result.failedToRegisterTableIdentifiers());
    writeToFile(
        outputDirPath.resolve(FAILED_TO_DELETE_AT_SOURCE_FILE),
        result.failedToDeleteTableIdentifiers());

    consoleLog.info("Finished {} ...", operation());
    printSummary(result);
    printDetails(result);
  }

  private void handleDryRunResult(List<TableIdentifier> identifiers) {
    writeToFile(outputDirPath.resolve(DRY_RUN_FILE), identifiers);
    consoleLog.info("Dry run is completed.");
    printDryRunResult(identifiers);
  }

  private void printSummary(CatalogMigrationResult result) {
    consoleLog.info("Summary: ");
    if (!result.registeredTableIdentifiers().isEmpty()) {
      consoleLog.info(
          "Successfully {} {} tables from {} catalog to {} catalog.",
          operated(),
          result.registeredTableIdentifiers().size(),
          sourceCatalogOptions.type.name(),
          targetCatalogOptions.type.name());
    }
    if (!result.failedToRegisterTableIdentifiers().isEmpty()) {
      consoleLog.info(
          "Failed to {} {} tables from {} catalog to {} catalog. "
              + "Please check the `catalog_migration.log` file for the failure reason. "
              + "Failed identifiers are written into `{}`. "
              + "Retry with that file using `--identifiers-from-file` option "
              + "if the failure is because of network/connection timeouts.",
          operate(),
          result.failedToRegisterTableIdentifiers().size(),
          sourceCatalogOptions.type.name(),
          targetCatalogOptions.type.name(),
          FAILED_IDENTIFIERS_FILE);
    }
    if (!result.failedToDeleteTableIdentifiers().isEmpty()) {
      consoleLog.info(
          "Failed to delete {} tables from {} catalog. "
              + "Please check the `catalog_migration.log` file for the reason. "
              + "{}Failed to delete identifiers are written into `{}`.",
          result.failedToDeleteTableIdentifiers().size(),
          sourceCatalogOptions.type.name(),
          System.lineSeparator(),
          FAILED_TO_DELETE_AT_SOURCE_FILE);
    }
  }

  private void printDetails(CatalogMigrationResult result) {
    consoleLog.info("Details: ");
    if (!result.registeredTableIdentifiers().isEmpty()) {
      consoleLog.info(
          "Successfully {} these tables:{}{}",
          operated(),
          System.lineSeparator(),
          result.registeredTableIdentifiers());
    }

    if (!result.failedToRegisterTableIdentifiers().isEmpty()) {
      consoleLog.info(
          "Failed to {} these tables:{}{}",
          operate(),
          System.lineSeparator(),
          result.failedToRegisterTableIdentifiers());
    }

    if (!result.failedToDeleteTableIdentifiers().isEmpty()) {
      consoleLog.warn(
          "{}Failed to delete these tables from source catalog:{}{}",
          ANSI_YELLOW,
          System.lineSeparator(),
          result.failedToDeleteTableIdentifiers());
    }
  }

  private void printDryRunResult(List<TableIdentifier> result) {
    consoleLog.info("Summary: ");
    if (result.isEmpty()) {
      consoleLog.info(
          "No tables are identified for {}. Please check logs for more info.", operation());
      return;
    }
    consoleLog.info(
        "Identified {} tables for {} by dry-run. These identifiers are also written into {}. "
            + "You can use this file with `--identifiers-from-file` option.",
        result.size(),
        operation(),
        DRY_RUN_FILE);

    consoleLog.info(
        "Details: {}Identified these tables for {} by dry-run:{}{}",
        System.lineSeparator(),
        operation(),
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
