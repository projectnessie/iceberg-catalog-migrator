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

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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

  public static final String FAILED_IDENTIFIERS_FILE = "failed_identifiers.txt";
  public static final String FAILED_TO_DELETE_AT_SOURCE_FILE = "failed_to_delete_at_source.txt";
  public static final String DRY_RUN_FILE = "dry_run_identifiers.txt";

  private CatalogMigrator() {}

  /**
   * Register or Migrate tables from one catalog(source catalog) to another catalog(target catalog).
   *
   * <p>Users must make sure that no in-progress commits on the tables of source catalog during
   * registration.
   *
   * @param catalogMigratorParams configuration params
   * @return List of successfully registered/migrated and list of failed to register/migrate table
   *     identifiers.
   */
  public static CatalogMigrationResult registerTables(CatalogMigratorParams catalogMigratorParams) {

    PrintWriter printWriter = catalogMigratorParams.printWriter();
    boolean deleteEntriesFromSourceCatalog = catalogMigratorParams.deleteEntriesFromSourceCatalog();
    String operation = deleteEntriesFromSourceCatalog ? "migration" : "registration";

    List<TableIdentifier> identifiers;
    if (catalogMigratorParams.tableIdentifiers() == null
        || catalogMigratorParams.tableIdentifiers().isEmpty()) {
      identifiers =
          getMatchingTableIdentifiers(
              catalogMigratorParams.sourceCatalog(),
              catalogMigratorParams.identifierRegex(),
              printWriter);
    } else {
      identifiers = catalogMigratorParams.tableIdentifiers();
    }

    printWriter.println(
        String.format("%nIdentified %d tables for %s.", identifiers.size(), operation));

    if (catalogMigratorParams.isDryRun()) {
      CatalogMigrationResult result =
          ImmutableCatalogMigrationResult.builder()
              .registeredTableIdentifiers(identifiers)
              .failedToRegisterTableIdentifiers(Collections.emptyList())
              .failedToDeleteTableIdentifiers(Collections.emptyList())
              .build();
      printWriter.println("Dry run is completed.");

      writeToFile(
          pathWithOutputDir(catalogMigratorParams.outputDirPath(), DRY_RUN_FILE),
          result.registeredTableIdentifiers());
      result.printDryRunResults(printWriter, deleteEntriesFromSourceCatalog);
      return result;
    }

    if (deleteEntriesFromSourceCatalog
        && catalogMigratorParams.sourceCatalog() instanceof HadoopCatalog) {
      printWriter.println(
          String.format(
              "[WARNING]: Source catalog type is HADOOP and it doesn't support dropping tables just from "
                  + "catalog. %nAvoid operating the migrated tables from the source catalog after migration. "
                  + "Use the tables from target catalog."));
    }

    printWriter.println(String.format("%nStarted %s ...", operation));
    List<TableIdentifier> registeredTableIdentifiers = new ArrayList<>();
    List<TableIdentifier> failedToRegisterTableIdentifiers = new ArrayList<>();
    List<TableIdentifier> failedToDeleteTableIdentifiers = new ArrayList<>();
    AtomicInteger counter = new AtomicInteger();
    identifiers.forEach(
        tableIdentifier -> {
          boolean isRegistered =
              registerTable(
                  catalogMigratorParams.sourceCatalog(),
                  catalogMigratorParams.targetCatalog(),
                  registeredTableIdentifiers,
                  failedToRegisterTableIdentifiers,
                  tableIdentifier);

          // HadoopCatalog dropTable will delete the table files completely even when purge is
          // false. So, skip dropTable for HadoopCatalog.
          boolean deleteTableFromSourceCatalog =
              !(catalogMigratorParams.sourceCatalog() instanceof HadoopCatalog)
                  && isRegistered
                  && deleteEntriesFromSourceCatalog;
          try {
            if (deleteTableFromSourceCatalog) {
              boolean isDropped =
                  catalogMigratorParams.sourceCatalog().dropTable(tableIdentifier, false);
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
                    "%nAttempted %s for %d tables out of %d tables.",
                    operation, count, identifiers.size()));
          }
        });
    printWriter.println(String.format("%nFinished %s ...", operation));

    CatalogMigrationResult result =
        ImmutableCatalogMigrationResult.builder()
            .registeredTableIdentifiers(registeredTableIdentifiers)
            .failedToRegisterTableIdentifiers(failedToRegisterTableIdentifiers)
            .failedToDeleteTableIdentifiers(failedToDeleteTableIdentifiers)
            .build();

    if (!result.failedToRegisterTableIdentifiers().isEmpty()) {
      writeToFile(
          pathWithOutputDir(catalogMigratorParams.outputDirPath(), FAILED_IDENTIFIERS_FILE),
          result.failedToRegisterTableIdentifiers());
    }
    if (!result.failedToDeleteTableIdentifiers().isEmpty()) {
      writeToFile(
          pathWithOutputDir(catalogMigratorParams.outputDirPath(), FAILED_TO_DELETE_AT_SOURCE_FILE),
          result.failedToDeleteTableIdentifiers());
    }

    result.printSummary(
        printWriter,
        deleteEntriesFromSourceCatalog,
        catalogMigratorParams.sourceCatalog().name(),
        catalogMigratorParams.targetCatalog().name());

    result.printDetails(printWriter, deleteEntriesFromSourceCatalog);

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
          String.format(
              "%nUser has not specified the table identifiers."
                  + " Selecting all the tables from all the namespaces from the source catalog."));
    } else {
      printWriter.println(
          String.format(
              "%nUser has not specified the table identifiers."
                  + " Selecting all the tables from all the namespaces from the source catalog "
                  + "which matches the regex pattern:"
                  + identifierRegex));
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

  private static Path pathWithOutputDir(String outputDirPath, String fileName) {
    if (outputDirPath == null) {
      return Paths.get(fileName);
    }
    return Paths.get(outputDirPath, fileName).toAbsolutePath();
  }

  private static void writeToFile(Path filePath, List<TableIdentifier> identifiers) {
    List<String> identifiersString =
        identifiers.stream().map(TableIdentifier::toString).collect(Collectors.toList());
    try {
      Files.write(filePath, identifiersString);
    } catch (IOException e) {
      throw new RuntimeException("Failed to write the file:" + filePath, e);
    }
  }
}
