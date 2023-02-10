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
package org.projectnessie.tools.catalog.migration;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogMigrator {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogMigrator.class);

  static final String FAILED_IDENTIFIERS_FILE = "failed_identifiers.txt";
  static final String FAILED_TO_DELETE_AT_SOURCE_FILE = "failed_to_delete_at_source.txt";
  static final String DRY_RUN_FILE = "dry_run_identifiers.txt";

  private CatalogMigrator() {}

  /**
   * Migrates tables from one catalog(source catalog) to another catalog(target catalog). After
   * successful migration, deletes the table entry from source catalog(not applicable for
   * HadoopCatalog).
   *
   * <p>Users must make sure that no in-progress commits on the tables of source catalog during
   * migration.
   *
   * @param tableIdentifiers a list of {@link TableIdentifier} for the tables required to be
   *     migrated. If not specified, all the tables would be migrated
   * @param sourceCatalog Source {@link Catalog} from which the tables are chosen
   * @param targetCatalog Target {@link Catalog} to which the tables need to be migrated
   * @param identifierRegex regular expression pattern used to migrate only the tables whose
   *     identifiers match this pattern. Can be provided instead of `tableIdentifiers`.
   * @param isDryRun to execute as dry run.
   * @param printWriter to print regular updates on the console.
   * @param outputDirPath optional path to store the result files. If null, uses present working
   *     directory.
   * @return List of successfully migrated and list of failed to migrate table identifiers.
   */
  public static CatalogMigrationResult migrateTables(
      List<TableIdentifier> tableIdentifiers,
      Catalog sourceCatalog,
      Catalog targetCatalog,
      String identifierRegex,
      boolean isDryRun,
      PrintWriter printWriter,
      String outputDirPath) {
    return registerTables(
        tableIdentifiers,
        sourceCatalog,
        targetCatalog,
        identifierRegex,
        isDryRun,
        printWriter,
        outputDirPath,
        true);
  }

  /**
   * Register tables from one catalog(source catalog) to another catalog(target catalog). User has
   * to take care of deleting the tables from source catalog after registration.
   *
   * <p>Users must make sure that no in-progress commits on the tables of source catalog during
   * registration.
   *
   * @param tableIdentifiers a list of {@link TableIdentifier} for the tables required to be
   *     registered. If not specified, all the tables would be registered
   * @param sourceCatalog Source {@link Catalog} from which the tables are chosen
   * @param targetCatalog Target {@link Catalog} to which the tables need to be registered
   * @param identifierRegex regular expression pattern used to migrate only the tables whose
   *     identifiers match this pattern. Can be provided instead of `tableIdentifiers`.
   * @param isDryRun to execute as dry run.
   * @param printWriter to print regular updates on the console.
   * @param outputDirPath optional path to store the result files. If null, uses present working
   *     directory.
   * @return List of successfully registered and list of failed to register table identifiers.
   */
  public static CatalogMigrationResult registerTables(
      List<TableIdentifier> tableIdentifiers,
      Catalog sourceCatalog,
      Catalog targetCatalog,
      String identifierRegex,
      boolean isDryRun,
      PrintWriter printWriter,
      String outputDirPath) {
    return registerTables(
        tableIdentifiers,
        sourceCatalog,
        targetCatalog,
        identifierRegex,
        isDryRun,
        printWriter,
        outputDirPath,
        false);
  }

  private static CatalogMigrationResult registerTables(
      List<TableIdentifier> tableIdentifiers,
      Catalog sourceCatalog,
      Catalog targetCatalog,
      String identifierRegex,
      boolean isDryRun,
      PrintWriter printWriter,
      String outputDirPath,
      boolean deleteEntriesFromSourceCatalog) {
    validate(sourceCatalog, targetCatalog);
    Preconditions.checkArgument(printWriter != null, "printWriter is null");

    if (identifierRegex != null && tableIdentifiers != null && !tableIdentifiers.isEmpty()) {
      throw new IllegalArgumentException(
          "Both the identifiers list and identifierRegex is configured.");
    }

    if (!isDryRun) {
      if (deleteEntriesFromSourceCatalog) {
        if (!proceedForRegistration(printWriter)) {
          return CatalogMigrator.CatalogMigrationResult.empty();
        }
      } else {
        if (!proceedForMigration(printWriter)) {
          return CatalogMigrator.CatalogMigrationResult.empty();
        }
      }
    }

    String operation = deleteEntriesFromSourceCatalog ? "migration" : "registration";

    List<TableIdentifier> identifiers;
    if (tableIdentifiers == null || tableIdentifiers.isEmpty()) {
      identifiers = getMatchingTableIdentifiers(sourceCatalog, identifierRegex, printWriter);
    } else {
      identifiers = tableIdentifiers;
    }

    printWriter.println(
        String.format("\nIdentified %d tables for %s.", identifiers.size(), operation));

    if (isDryRun) {
      CatalogMigrationResult result =
          new CatalogMigrationResult(identifiers, Collections.emptyList(), Collections.emptyList());
      printWriter.println("Dry run is completed.");

      writeToFile(
          pathWithOutputDir(outputDirPath, DRY_RUN_FILE), result.registeredTableIdentifiers());
      printDryRunResults(printWriter, result, deleteEntriesFromSourceCatalog);
      return result;
    }

    if (deleteEntriesFromSourceCatalog && sourceCatalog instanceof HadoopCatalog) {
      printWriter.println(
          "[WARNING]: Source catalog type is HADOOP and it doesn't support dropping tables just from "
              + "catalog. \nAvoid operating the migrated tables from the source catalog after migration. "
              + "Use the tables from target catalog.");
    }

    printWriter.println(String.format("\nStarted %s ...", operation));
    List<TableIdentifier> registeredTableIdentifiers = new ArrayList<>();
    List<TableIdentifier> failedToRegisterTableIdentifiers = new ArrayList<>();
    List<TableIdentifier> failedToDeleteTableIdentifiers = new ArrayList<>();
    AtomicInteger counter = new AtomicInteger();
    identifiers.forEach(
        tableIdentifier -> {
          boolean isRegistered =
              registerTable(
                  sourceCatalog,
                  targetCatalog,
                  registeredTableIdentifiers,
                  failedToRegisterTableIdentifiers,
                  tableIdentifier);

          // HadoopCatalog dropTable will delete the table files completely even when purge is
          // false. So, skip dropTable for HadoopCatalog.
          boolean deleteTableFromSourceCatalog =
              !(sourceCatalog instanceof HadoopCatalog)
                  && isRegistered
                  && deleteEntriesFromSourceCatalog;
          try {
            if (deleteTableFromSourceCatalog) {
              boolean isDropped = sourceCatalog.dropTable(tableIdentifier, false);
              if (!isDropped) {
                failedToDeleteTableIdentifiers.add(tableIdentifier);
              }
            }
          } catch (Exception exception) {
            failedToDeleteTableIdentifiers.add(tableIdentifier);
            LOG.warn("Failed to delete the table after migration {}", tableIdentifier, exception);
          }

          int count = counter.incrementAndGet();
          if (count % 100 == 0) {
            printWriter.println(
                String.format(
                    "\nAttempted %s for %d tables out of %d tables.",
                    operation, count, identifiers.size()));
          }
        });
    printWriter.println(String.format("\nFinished %s ...", operation));

    CatalogMigrationResult result =
        new CatalogMigrationResult(
            registeredTableIdentifiers,
            failedToRegisterTableIdentifiers,
            failedToDeleteTableIdentifiers);

    if (!result.failedToRegisterTableIdentifiers().isEmpty()) {
      writeToFile(
          pathWithOutputDir(outputDirPath, FAILED_IDENTIFIERS_FILE),
          result.failedToRegisterTableIdentifiers());
    }
    if (!result.failedToDeleteTableIdentifiers().isEmpty()) {
      writeToFile(
          pathWithOutputDir(outputDirPath, FAILED_TO_DELETE_AT_SOURCE_FILE),
          result.failedToDeleteTableIdentifiers());
    }

    printSummary(
        printWriter,
        result,
        deleteEntriesFromSourceCatalog,
        sourceCatalog.name(),
        targetCatalog.name());

    printDetails(printWriter, result, deleteEntriesFromSourceCatalog);

    return result;
  }

  private static boolean registerTable(
      Catalog sourceCatalog,
      Catalog targetCatalog,
      List<TableIdentifier> registeredTableIdentifiers,
      List<TableIdentifier> failedToMigrateTableIdentifiers,
      TableIdentifier tableIdentifier) {
    try {
      // register the table to the target catalog
      TableOperations ops = ((BaseTable) sourceCatalog.loadTable(tableIdentifier)).operations();
      targetCatalog.registerTable(tableIdentifier, ops.current().metadataFileLocation());

      registeredTableIdentifiers.add(tableIdentifier);
      LOG.info("Successfully migrated the table {}", tableIdentifier);
      return true;
    } catch (Exception ex) {
      failedToMigrateTableIdentifiers.add(tableIdentifier);
      LOG.warn("Unable to register the table {}", tableIdentifier, ex);
      return false;
    }
  }

  private static List<TableIdentifier> getMatchingTableIdentifiers(
      Catalog sourceCatalog, String identifierRegex, PrintWriter printWriter) {
    if (identifierRegex == null) {
      printWriter.println(
          "\nUser has not specified the table identifiers."
              + " Selecting all the tables from all the namespaces from the source catalog.");
    } else {
      printWriter.println(
          "\nUser has not specified the table identifiers."
              + " Selecting all the tables from all the namespaces from the source catalog "
              + "which matches the regex pattern:"
              + identifierRegex);
    }

    printWriter.println("Collecting all the namespaces from source catalog...");
    // fetch all the table identifiers from all the namespaces.
    List<Namespace> namespaces =
        (sourceCatalog instanceof SupportsNamespaces)
            ? ((SupportsNamespaces) sourceCatalog).listNamespaces()
            : ImmutableList.of(Namespace.empty());
    if (identifierRegex == null) {
      printWriter.println("Collecting all the tables from all the namespaces of source catalog...");
    } else {
      printWriter.println(
          "Collecting all the tables from all the namespaces of source catalog"
              + " which matches the regex pattern:"
              + identifierRegex);
    }

    Predicate<TableIdentifier> matchedIdentifiersPredicate;
    if (identifierRegex != null) {
      Pattern pattern = Pattern.compile(identifierRegex);
      matchedIdentifiersPredicate =
          tableIdentifier -> pattern.matcher(tableIdentifier.toString()).matches();
    } else {
      matchedIdentifiersPredicate = tableIdentifier -> true;
    }
    return getMatchingTableIdentifiers(sourceCatalog, namespaces, matchedIdentifiersPredicate);
  }

  private static boolean proceedForRegistration(PrintWriter printWriter) {
    String warning =
        "\n[WARNING]\n"
            + "\ta) Executing catalog migration when the source catalog has some in-progress commits "
            + "\n\tcan lead to a data loss as the in-progress commit will not be considered for migration. "
            + "\n\tSo, while using this tool please make sure there are no in-progress commits for the source "
            + "catalog\n"
            + "\n"
            + "\tb) After the registration, successfully registered tables will be present in both source and target "
            + "catalog. "
            + "\n\tHaving the same metadata.json registered in more than one catalog can lead to missing updates, "
            + "loss of data, and table corruption. "
            + "\n\tUse `--delete-source-tables` option to automatically delete the table from source catalog after "
            + "migration.";
    return proceed(warning, printWriter);
  }

  private static boolean proceedForMigration(PrintWriter printWriter) {
    String warning =
        "\n[WARNING]\n"
            + "\ta) Executing catalog migration when the source catalog has some in-progress commits "
            + "\n\tcan lead to a data loss as the in-progress commit will not be considered for migration. "
            + "\n\tSo, while using this tool please make sure there are no in-progress commits for the source "
            + "catalog\n"
            + "\n"
            + "\tb) After the migration, successfully migrated tables will be deleted from the source catalog "
            + "\n\tand can only be accessed from the target catalog.";
    return proceed(warning, printWriter);
  }

  private static boolean proceed(String warning, PrintWriter printWriter) {
    try (Scanner scanner = new Scanner(System.in)) {
      printWriter.println(warning);

      while (true) {
        printWriter.println(
            "Have you read the above warnings and are you sure you want to continue? (yes/no):");
        String input = scanner.nextLine();

        if (input.equalsIgnoreCase("yes")) {
          printWriter.println("Continuing...");
          return true;
        } else if (input.equalsIgnoreCase("no")) {
          printWriter.println("Aborting...");
          return false;
        } else {
          printWriter.println("Invalid input. Please enter 'yes' or 'no'.");
        }
      }
    }
  }

  private static String pathWithOutputDir(String outputDirPath, String fileName) {
    if (outputDirPath == null) {
      return fileName;
    }
    if (outputDirPath.endsWith("/")) {
      return outputDirPath + fileName;
    }
    return outputDirPath + "/" + fileName;
  }

  private static List<TableIdentifier> getMatchingTableIdentifiers(
      Catalog sourceCatalog,
      List<Namespace> namespaces,
      Predicate<TableIdentifier> matchedIdentifiersPredicate) {
    List<TableIdentifier> allIdentifiers = new ArrayList<>();
    namespaces.stream()
        .filter(Objects::nonNull)
        .forEach(
            namespace -> {
              List<TableIdentifier> matchedIdentifiers =
                  sourceCatalog.listTables(namespace).stream()
                      .filter(matchedIdentifiersPredicate)
                      .collect(Collectors.toList());
              allIdentifiers.addAll(matchedIdentifiers);
            });
    return allIdentifiers;
  }

  private static void validate(Catalog sourceCatalog, Catalog targetCatalog) {
    Preconditions.checkArgument(sourceCatalog != null, "Invalid source catalog: null");
    Preconditions.checkArgument(targetCatalog != null, "Invalid target catalog: null");
    Preconditions.checkArgument(
        !targetCatalog.equals(sourceCatalog), "target catalog is same as source catalog");
  }

  private static void printSummary(
      PrintWriter printWriter,
      CatalogMigrator.CatalogMigrationResult result,
      boolean deleteSourceCatalogTables,
      String sourceCatalogType,
      String targetCatalogType) {
    printWriter.println("\nSummary: ");
    if (!result.registeredTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- Successfully %s %d tables from %s catalog to %s catalog.",
              deleteSourceCatalogTables ? "migrated" : "registered",
              result.registeredTableIdentifiers().size(),
              sourceCatalogType,
              targetCatalogType));
    }
    if (!result.failedToRegisterTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- Failed to %s %d tables from %s catalog to %s catalog. "
                  + "Please check the `catalog_migration.log` file for the failure reason. "
                  + "\nFailed identifiers are written into `%s`. "
                  + "Retry with that file using `--identifiers-from-file` option "
                  + "if the failure is because of network/connection timeouts.",
              deleteSourceCatalogTables ? "migrate" : "register",
              result.failedToRegisterTableIdentifiers().size(),
              sourceCatalogType,
              targetCatalogType,
              FAILED_IDENTIFIERS_FILE));
    }
    if (!result.failedToDeleteTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- Failed to delete %d tables from %s catalog. "
                  + "Please check the `catalog_migration.log` file for the reason. "
                  + "\nFailed to delete identifiers are written into `%s`. ",
              result.failedToDeleteTableIdentifiers().size(),
              sourceCatalogType,
              FAILED_TO_DELETE_AT_SOURCE_FILE));
    }
  }

  private static void printDetails(
      PrintWriter printWriter,
      CatalogMigrator.CatalogMigrationResult result,
      boolean deleteSourceCatalogTables) {
    printWriter.println("\nDetails: ");
    if (!result.registeredTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- Successfully %s these tables:",
              deleteSourceCatalogTables ? "migrated" : "registered"));
      printWriter.println(result.registeredTableIdentifiers());
    }

    if (!result.failedToRegisterTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- Failed to %s these tables:", deleteSourceCatalogTables ? "migrate" : "register"));
      printWriter.println(result.failedToRegisterTableIdentifiers());
    }

    if (!result.failedToDeleteTableIdentifiers().isEmpty()) {
      printWriter.println("- [WARNING] Failed to delete these tables from source catalog:");
      printWriter.println(result.failedToDeleteTableIdentifiers());
    }
  }

  private static void printDryRunResults(
      PrintWriter printWriter,
      CatalogMigrator.CatalogMigrationResult result,
      boolean deleteSourceCatalogTables) {
    printWriter.println("\nSummary: ");
    if (result.registeredTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- No tables are identified for %s. Please check logs for more info.",
              deleteSourceCatalogTables ? "migration" : "registration"));
      return;
    }
    printWriter.println(
        String.format(
            "- Identified %d tables for %s by dry-run. These identifiers are also written into %s. "
                + "You can use this file with `--identifiers-from-file` option.",
            result.registeredTableIdentifiers().size(),
            deleteSourceCatalogTables ? "migration" : "registration",
            DRY_RUN_FILE));

    printWriter.println("\nDetails: ");
    printWriter.println(
        String.format(
            "- Identified these tables for %s by dry-run:",
            deleteSourceCatalogTables ? "migration" : "registration"));
    printWriter.println(result.registeredTableIdentifiers());
  }

  private static void writeToFile(String filePath, List<TableIdentifier> identifiers) {
    List<String> identifiersString =
        identifiers.stream().map(TableIdentifier::toString).collect(Collectors.toList());
    try {
      Files.write(Paths.get(filePath), identifiersString);
    } catch (IOException e) {
      throw new RuntimeException("Failed to write the file:" + filePath, e);
    }
  }

  public static class CatalogMigrationResult {
    private final List<TableIdentifier> registeredTableIdentifiers;
    private final List<TableIdentifier> failedToRegisterTableIdentifiers;
    private final List<TableIdentifier> failedToDeleteTableIdentifiers;

    CatalogMigrationResult(
        List<TableIdentifier> registeredTableIdentifiers,
        List<TableIdentifier> failedToRegisterTableIdentifiers,
        List<TableIdentifier> failedToDeleteTableIdentifiers) {
      this.registeredTableIdentifiers = registeredTableIdentifiers;
      this.failedToRegisterTableIdentifiers = failedToRegisterTableIdentifiers;
      this.failedToDeleteTableIdentifiers = failedToDeleteTableIdentifiers;
    }

    public List<TableIdentifier> registeredTableIdentifiers() {
      return registeredTableIdentifiers;
    }

    public List<TableIdentifier> failedToRegisterTableIdentifiers() {
      return failedToRegisterTableIdentifiers;
    }

    public List<TableIdentifier> failedToDeleteTableIdentifiers() {
      return failedToDeleteTableIdentifiers;
    }

    public static CatalogMigrationResult empty() {
      return new CatalogMigrationResult(
          Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }
  }
}
