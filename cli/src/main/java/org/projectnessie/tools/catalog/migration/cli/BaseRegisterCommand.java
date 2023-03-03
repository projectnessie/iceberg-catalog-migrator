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
import java.io.PrintWriter;
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
import picocli.CommandLine;

public abstract class BaseRegisterCommand implements Callable<Integer> {
  @CommandLine.Spec CommandLine.Model.CommandSpec commandSpec;

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

    PrintWriter printWriter = commandSpec.commandLine().getOut();

    Catalog sourceCatalog = sourceCatalogOptions.build();
    printWriter.printf("%nConfigured source catalog: %s%n", sourceCatalog.name());

    Catalog targetCatalog = targetCatalogOptions.build();
    printWriter.printf("%nConfigured target catalog: %s%n", targetCatalog.name());

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
        printWriter.printf(
            "%nUser has not specified the table identifiers."
                + " Selecting all the tables from all the namespaces from the source catalog "
                + "which matches the regex pattern:%s%n",
            identifierRegEx);
      } else {
        printWriter.printf(
            "%nUser has not specified the table identifiers."
                + " Selecting all the tables from all the namespaces from the source catalog.%n");
      }
      identifiers = catalogMigrator.getMatchingTableIdentifiers(identifierRegEx);
    }

    String operation = deleteSourceCatalogTables ? "migration" : "registration";
    printWriter.printf("%nIdentified %d tables for %s.%n", identifiers.size(), operation);

    if (isDryRun) {
      writeToFile(outputDirPath.resolve(DRY_RUN_FILE), identifiers);
      printWriter.println("Dry run is completed.");
      printDryRunResults(identifiers);
      return 0;
    }

    printWriter.printf("%nStarted %s ...%n", operation);

    List<List<TableIdentifier>> identifierBatches = Lists.partition(identifiers, BATCH_SIZE);
    int totalIdentifiers = identifiers.size();
    AtomicInteger counter = new AtomicInteger();
    identifierBatches.forEach(
        identifierBatch -> {
          catalogMigrator.registerTables(identifierBatch);
          printWriter.printf(
              "%nAttempted %s for %d tables out of %d tables.%n",
              operation, counter.addAndGet(identifierBatch.size()), totalIdentifiers);
        });

    CatalogMigrationResult result = catalogMigrator.result();
    writeToFile(
        outputDirPath.resolve(FAILED_IDENTIFIERS_FILE), result.failedToRegisterTableIdentifiers());
    writeToFile(
        outputDirPath.resolve(FAILED_TO_DELETE_AT_SOURCE_FILE),
        result.failedToDeleteTableIdentifiers());

    printWriter.printf("%nFinished %s ...%n", operation);
    printSummary(result, sourceCatalog.name(), targetCatalog.name());
    printDetails(result);
    return 0;
  }

  private boolean canProceed(Catalog sourceCatalog) {
    if (isDryRun || disablePrompts) {
      return true;
    }
    PrintWriter printWriter = commandSpec.commandLine().getOut();
    if (deleteSourceCatalogTables) {
      if (sourceCatalog instanceof HadoopCatalog) {
        printWriter.printf(
            "[WARNING]: Source catalog type is HADOOP and it doesn't support dropping tables just from "
                + "catalog. %nAvoid operating the migrated tables from the source catalog after migration. "
                + "Use the tables from target catalog.%n");
      }
      return PromptUtil.proceedForMigration(printWriter);
    } else {
      return PromptUtil.proceedForRegistration(printWriter);
    }
  }

  private void printSummary(
      CatalogMigrationResult result, String sourceCatalogName, String targetCatalogName) {
    PrintWriter printWriter = commandSpec.commandLine().getOut();
    printWriter.printf("%nSummary: %n");
    if (!result.registeredTableIdentifiers().isEmpty()) {
      printWriter.printf(
          "- Successfully %s %d tables from %s catalog to %s catalog.%n",
          deleteSourceCatalogTables ? "migrated" : "registered",
          result.registeredTableIdentifiers().size(),
          sourceCatalogName,
          targetCatalogName);
    }
    if (!result.failedToRegisterTableIdentifiers().isEmpty()) {
      printWriter.printf(
          "- Failed to %s %d tables from %s catalog to %s catalog. "
              + "Please check the `catalog_migration.log` file for the failure reason. "
              + "%nFailed identifiers are written into `%s`. "
              + "Retry with that file using `--identifiers-from-file` option "
              + "if the failure is because of network/connection timeouts.%n",
          deleteSourceCatalogTables ? "migrate" : "register",
          result.failedToRegisterTableIdentifiers().size(),
          sourceCatalogName,
          targetCatalogName,
          FAILED_IDENTIFIERS_FILE);
    }
    if (!result.failedToDeleteTableIdentifiers().isEmpty()) {
      printWriter.printf(
          "- Failed to delete %d tables from %s catalog. "
              + "Please check the `catalog_migration.log` file for the reason. "
              + "%nFailed to delete identifiers are written into `%s`. %n",
          result.failedToDeleteTableIdentifiers().size(),
          sourceCatalogName,
          FAILED_TO_DELETE_AT_SOURCE_FILE);
    }
  }

  private void printDetails(CatalogMigrationResult result) {
    PrintWriter printWriter = commandSpec.commandLine().getOut();
    printWriter.printf("%nDetails: %n");
    if (!result.registeredTableIdentifiers().isEmpty()) {
      printWriter.printf(
          "- Successfully %s these tables:%n",
          deleteSourceCatalogTables ? "migrated" : "registered");
      printWriter.println(result.registeredTableIdentifiers());
    }

    if (!result.failedToRegisterTableIdentifiers().isEmpty()) {
      printWriter.printf(
          "- Failed to %s these tables:%n", deleteSourceCatalogTables ? "migrate" : "register");
      printWriter.println(result.failedToRegisterTableIdentifiers());
    }

    if (!result.failedToDeleteTableIdentifiers().isEmpty()) {
      printWriter.println("- [WARNING] Failed to delete these tables from source catalog:");
      printWriter.println(result.failedToDeleteTableIdentifiers());
    }
  }

  private void printDryRunResults(List<TableIdentifier> result) {
    PrintWriter printWriter = commandSpec.commandLine().getOut();
    printWriter.printf("%nSummary: %n");
    if (result.isEmpty()) {
      printWriter.printf(
          "- No tables are identified for %s. Please check logs for more info.%n",
          deleteSourceCatalogTables ? "migration" : "registration");
      return;
    }
    printWriter.printf(
        "- Identified %d tables for %s by dry-run. These identifiers are also written into %s. "
            + "You can use this file with `--identifiers-from-file` option.%n",
        result.size(), deleteSourceCatalogTables ? "migration" : "registration", DRY_RUN_FILE);

    printWriter.printf("%nDetails: %n");
    printWriter.printf(
        "- Identified these tables for %s by dry-run:%n",
        deleteSourceCatalogTables ? "migration" : "registration");
    printWriter.println(result);
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
