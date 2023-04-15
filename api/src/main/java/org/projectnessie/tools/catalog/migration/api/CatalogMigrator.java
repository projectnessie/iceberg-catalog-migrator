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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
public abstract class CatalogMigrator {

  /** Source {@link Catalog} from which the tables are chosen. */
  public abstract Catalog sourceCatalog();

  /** Target {@link Catalog} to which the tables need to be registered or migrated. */
  public abstract Catalog targetCatalog();

  /** Delete the table entries from the source catalog after successful registration. */
  public abstract boolean deleteEntriesFromSourceCatalog();

  /** Enable the stacktrace in logs in case of failures. */
  @Value.Default
  public boolean enableStacktrace() {
    return false;
  }

  @Value.Check
  void check() {
    if (!(targetCatalog() instanceof SupportsNamespaces)) {
      throw new UnsupportedOperationException(
          String.format(
              "target catalog %s doesn't implement SupportsNamespaces to create missing namespaces.",
              targetCatalog().name()));
    }

    if (deleteEntriesFromSourceCatalog() && sourceCatalog() instanceof HadoopCatalog) {
      throw new UnsupportedOperationException(
          "Source catalog is a Hadoop catalog and it doesn't support deleting the table entries just from the catalog. Please configure `deleteEntriesFromSourceCatalog` as `false`");
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(CatalogMigrator.class);
  private final ImmutableCatalogMigrationResult.Builder resultBuilder =
      ImmutableCatalogMigrationResult.builder();
  private final Set<Namespace> processedNamespaces = new HashSet<>();

  /**
   * Get the table identifiers which matches the regular expression pattern input from all the
   * namespaces.
   *
   * @param identifierRegex regular expression pattern. If null, fetches all the table identifiers
   *     from all the namespaces.
   * @return Set of table identifiers.
   */
  public Set<TableIdentifier> getMatchingTableIdentifiers(String identifierRegex) {
    Catalog sourceCatalog = sourceCatalog();
    if (!(sourceCatalog instanceof SupportsNamespaces)) {
      throw new UnsupportedOperationException(
          String.format(
              "source catalog %s doesn't implement SupportsNamespaces to list all namespaces.",
              sourceCatalog.name()));
    }
    LOG.info("Collecting all the namespaces from source catalog...");
    Set<Namespace> namespaces = new LinkedHashSet<>();
    getAllNamespacesFromSourceCatalog(Namespace.empty(), namespaces);

    Predicate<TableIdentifier> matchedIdentifiersPredicate;
    if (identifierRegex == null) {
      LOG.info("Collecting all the tables from all the namespaces of source catalog...");
      matchedIdentifiersPredicate = tableIdentifier -> true;
    } else {
      LOG.info(
          "Collecting all the tables from all the namespaces of source catalog"
              + " which matches the regex pattern:{}",
          identifierRegex);
      Pattern pattern = Pattern.compile(identifierRegex);
      matchedIdentifiersPredicate =
          tableIdentifier -> pattern.matcher(tableIdentifier.toString()).matches();
    }
    return namespaces.stream()
        .flatMap(
            namespace -> {
              try {
                return sourceCatalog.listTables(namespace).stream()
                    .filter(matchedIdentifiersPredicate);
              } catch (IllegalArgumentException | NoSuchNamespaceException exception) {
                if (namespace.isEmpty()) {
                  // some catalogs don't support default namespace.
                  // Hence, just log the warning and ignore the exception.
                  LOG.warn(
                      "Failed to identify tables from default namespace: {}",
                      exception.getMessage());
                  return Stream.empty();
                } else {
                  throw exception;
                }
              }
            })
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  /**
   * Register or Migrate tables from one catalog(source catalog) to another catalog(target catalog).
   *
   * <p>Users must make sure that no in-progress commits on the tables of source catalog during
   * registration.
   *
   * @param identifiers collection of table identifiers to register or migrate
   * @return {@code this} for use in a chained invocation
   */
  public CatalogMigrator registerTables(Collection<TableIdentifier> identifiers) {
    Preconditions.checkArgument(identifiers != null, "Identifiers list is null");

    if (identifiers.isEmpty()) {
      LOG.warn("Identifiers list is empty");
      return this;
    }

    identifiers.forEach(this::registerTable);
    return this;
  }

  /**
   * Register or Migrate a single table from one catalog(source catalog) to another catalog(target
   * catalog).
   *
   * <p>Users must make sure that no in-progress commits on the tables of source catalog during
   * registration.
   *
   * @param identifier table identifier to register or migrate
   * @return {@code this} for use in a chained invocation
   */
  public CatalogMigrator registerTable(TableIdentifier identifier) {
    Preconditions.checkArgument(identifier != null, "Identifier is null");

    boolean isRegistered = registerTableToTargetCatalog(identifier);
    if (isRegistered) {
      resultBuilder.addRegisteredTableIdentifiers(identifier);
    } else {
      resultBuilder.addFailedToRegisterTableIdentifiers(identifier);
    }

    try {
      if (isRegistered
          && deleteEntriesFromSourceCatalog()
          && !sourceCatalog().dropTable(identifier, false)) {
        resultBuilder.addFailedToDeleteTableIdentifiers(identifier);
      }
    } catch (Exception exception) {
      resultBuilder.addFailedToDeleteTableIdentifiers(identifier);
      if (enableStacktrace()) {
        LOG.error("Failed to delete the table after migration {}", identifier, exception);
      } else {
        LOG.error(
            "Failed to delete the table after migration {} : {}",
            identifier,
            exception.getMessage());
      }
    }
    return this;
  }

  public CatalogMigrationResult result() {
    processedNamespaces.clear();
    return resultBuilder.build();
  }

  protected void createNamespacesIfNotExistOnTargetCatalog(Namespace identifierNamespace) {
    if (!processedNamespaces.contains(identifierNamespace)) {
      String[] levels = identifierNamespace.levels();
      for (int index = 0; index < levels.length; index++) {
        Namespace namespace = Namespace.of(Arrays.copyOfRange(levels, 0, index + 1));
        if (processedNamespaces.add(namespace)) {
          try {
            ((SupportsNamespaces) targetCatalog()).createNamespace(namespace);
          } catch (AlreadyExistsException ex) {
            LOG.debug(
                "{}.Ignoring the error as forcefully creating the namespace even if it exists to avoid "
                    + "namespaceExists() check.",
                ex.getMessage());
          }
        }
      }
    }
  }

  protected void getAllNamespacesFromSourceCatalog(Namespace namespace, Set<Namespace> visited) {
    if (!visited.add(namespace)) {
      return;
    }
    List<Namespace> children = ((SupportsNamespaces) sourceCatalog()).listNamespaces(namespace);
    for (Namespace child : children) {
      getAllNamespacesFromSourceCatalog(child, visited);
    }
  }

  private boolean registerTableToTargetCatalog(TableIdentifier tableIdentifier) {
    try {
      createNamespacesIfNotExistOnTargetCatalog(tableIdentifier.namespace());
      // register the table to the target catalog
      TableOperations ops = ((BaseTable) sourceCatalog().loadTable(tableIdentifier)).operations();
      targetCatalog().registerTable(tableIdentifier, ops.current().metadataFileLocation());
      LOG.info("Successfully registered the table {}", tableIdentifier);
      return true;
    } catch (Exception ex) {
      if (enableStacktrace()) {
        LOG.error("Unable to register the table {}", tableIdentifier, ex);
      } else {
        LOG.error("Unable to register the table {} : {}", tableIdentifier, ex.getMessage());
      }
      return false;
    }
  }
}
