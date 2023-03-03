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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;
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

public abstract class AbstractCatalogMigrator {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractCatalogMigrator.class);
  private final ImmutableCatalogMigrationResult.Builder resultBuilder =
      ImmutableCatalogMigrationResult.builder();

  public abstract CatalogMigratorParams getParams();

  /**
   * Get the table identifiers which matches the regular expression pattern input from all the
   * namespaces.
   *
   * @param identifierRegex regular expression pattern. If null, fetches all the table identifiers
   *     from all the namespaces.
   * @return List of table identifiers.
   */
  public List<TableIdentifier> getMatchingTableIdentifiers(String identifierRegex) {
    Catalog sourceCatalog = getParams().sourceCatalog();
    if (!(sourceCatalog instanceof SupportsNamespaces)) {
      throw new UnsupportedOperationException(
          String.format(
              "source catalog %s doesn't implement SupportsNamespaces to list all namespaces.",
              sourceCatalog.name()));
    }
    LOG.info("Collecting all the namespaces from source catalog...");
    List<Namespace> namespaces = ((SupportsNamespaces) sourceCatalog).listNamespaces();
    Predicate<TableIdentifier> matchedIdentifiersPredicate;
    if (identifierRegex == null) {
      LOG.info("Collecting all the tables from all the namespaces of source catalog...");
      matchedIdentifiersPredicate = tableIdentifier -> true;
    } else {
      LOG.info(
          "Collecting all the tables from all the namespaces of source catalog"
              + " which matches the regex pattern:"
              + identifierRegex);
      Pattern pattern = Pattern.compile(identifierRegex);
      matchedIdentifiersPredicate =
          tableIdentifier -> pattern.matcher(tableIdentifier.toString()).matches();
    }
    return namespaces.stream()
        .filter(Objects::nonNull)
        .flatMap(
            namespace ->
                sourceCatalog.listTables(namespace).stream().filter(matchedIdentifiersPredicate))
        .collect(Collectors.toList());
  }

  /**
   * Register or Migrate tables from one catalog(source catalog) to another catalog(target catalog).
   *
   * <p>Users must make sure that no in-progress commits on the tables of source catalog during
   * registration.
   *
   * @param identifiers List of table identifiers to register or migrate
   * @return {@code this} for use in a chained invocation
   */
  public AbstractCatalogMigrator registerTables(List<TableIdentifier> identifiers) {
    Preconditions.checkArgument(identifiers != null, "Identifiers list is null");

    if (identifiers.isEmpty()) {
      LOG.info("Identifiers list is empty");
      return this;
    }

    identifiers.forEach(
        tableIdentifier -> {
          boolean isRegistered = registerTable(tableIdentifier);
          if (isRegistered) {
            resultBuilder.addRegisteredTableIdentifiers(tableIdentifier);
          } else {
            resultBuilder.addFailedToRegisterTableIdentifiers(tableIdentifier);
          }

          // HadoopCatalog dropTable will delete the table files completely even when purge is
          // false. So, skip dropTable for HadoopCatalog.
          boolean deleteTableFromSourceCatalog =
              !(getParams().sourceCatalog() instanceof HadoopCatalog)
                  && isRegistered
                  && getParams().deleteEntriesFromSourceCatalog();
          try {
            if (deleteTableFromSourceCatalog
                && !getParams().sourceCatalog().dropTable(tableIdentifier, false)) {
              resultBuilder.addFailedToDeleteTableIdentifiers(tableIdentifier);
            }
          } catch (Exception exception) {
            resultBuilder.addFailedToDeleteTableIdentifiers(tableIdentifier);
            LOG.warn("Failed to delete the table after migration {}", tableIdentifier, exception);
          }
        });
    return this;
  }

  public CatalogMigrationResult result() {
    return resultBuilder.build();
  }

  private boolean registerTable(TableIdentifier tableIdentifier) {
    try {
      // register the table to the target catalog
      TableOperations ops =
          ((BaseTable) getParams().sourceCatalog().loadTable(tableIdentifier)).operations();
      getParams()
          .targetCatalog()
          .registerTable(tableIdentifier, ops.current().metadataFileLocation());
      LOG.info("Successfully migrated the table {}", tableIdentifier);
      return true;
    } catch (Exception ex) {
      LOG.warn("Unable to register the table {}", tableIdentifier, ex);
      return false;
    }
  }
}
