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
import java.io.PrintWriter;
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
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogMigrateUtil {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogMigrateUtil.class);

  private CatalogMigrateUtil() {}

  /**
   * Migrates tables from one catalog(source catalog) to another catalog(target catalog). After
   * successful migration, deletes the table entry from source catalog(not applicable for
   * HadoopCatalog).
   *
   * <p>Supports bulk migrations with a multi-thread execution.
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
   * @return List of successfully migrated and list of failed to migrate table identifiers.
   */
  public static CatalogMigrationResult migrateTables(
      List<TableIdentifier> tableIdentifiers,
      Catalog sourceCatalog,
      Catalog targetCatalog,
      String identifierRegex,
      boolean isDryRun,
      PrintWriter printWriter) {
    return migrateTables(
        tableIdentifiers,
        sourceCatalog,
        targetCatalog,
        identifierRegex,
        isDryRun,
        true,
        printWriter);
  }

  /**
   * Register tables from one catalog(source catalog) to another catalog(target catalog). User has
   * to take care of deleting the tables from source catalog after registration.
   *
   * <p>Supports bulk registration with a multi-thread execution.
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
   * @return List of successfully migrated and list of failed to migrate table identifiers.
   */
  public static CatalogMigrationResult registerTables(
      List<TableIdentifier> tableIdentifiers,
      Catalog sourceCatalog,
      Catalog targetCatalog,
      String identifierRegex,
      boolean isDryRun,
      PrintWriter printWriter) {
    return migrateTables(
        tableIdentifiers,
        sourceCatalog,
        targetCatalog,
        identifierRegex,
        isDryRun,
        false,
        printWriter);
  }

  private static CatalogMigrationResult migrateTables(
      List<TableIdentifier> tableIdentifiers,
      Catalog sourceCatalog,
      Catalog targetCatalog,
      String identifierRegex,
      boolean isDryRun,
      boolean deleteEntriesFromSourceCatalog,
      PrintWriter printWriter) {
    validate(sourceCatalog, targetCatalog);

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
      return new CatalogMigrationResult(
          identifiers, Collections.emptyList(), Collections.emptyList());
    }
    printWriter.println(String.format("\nStarted %s ...", operation));
    List<TableIdentifier> registeredTableIdentifiers = new ArrayList<>();
    List<TableIdentifier> failedToRegisterTableIdentifiers = new ArrayList<>();
    List<TableIdentifier> failedToDeleteTableIdentifiers = new ArrayList<>();
    AtomicInteger counter = new AtomicInteger();
    identifiers.forEach(
        tableIdentifier -> {
          registerTable(
              sourceCatalog,
              targetCatalog,
              registeredTableIdentifiers,
              failedToRegisterTableIdentifiers,
              tableIdentifier);

          // HadoopCatalog dropTable will delete the table files completely even when purge is
          // false.
          // So, skip dropTable for HadoopCatalog.
          boolean deleteTableFromSourceCatalog =
              deleteEntriesFromSourceCatalog && !(sourceCatalog instanceof HadoopCatalog);

          try {
            if (deleteTableFromSourceCatalog) {
              boolean failedToDelete = sourceCatalog.dropTable(tableIdentifier, false);
              if (failedToDelete) {
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
    return new CatalogMigrationResult(
        registeredTableIdentifiers,
        failedToRegisterTableIdentifiers,
        failedToDeleteTableIdentifiers);
  }

  private static void registerTable(
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
    } catch (Exception ex) {
      failedToMigrateTableIdentifiers.add(tableIdentifier);
      LOG.warn("Unable to migrate table {}", tableIdentifier, ex);
    }
  }

  @NotNull
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
  }
}
