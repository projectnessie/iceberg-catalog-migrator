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

import static org.projectnessie.tools.catalog.migration.api.CatalogMigrator.DRY_RUN_FILE;
import static org.projectnessie.tools.catalog.migration.api.CatalogMigrator.FAILED_IDENTIFIERS_FILE;
import static org.projectnessie.tools.catalog.migration.api.CatalogMigrator.FAILED_TO_DELETE_AT_SOURCE_FILE;

import java.io.PrintWriter;
import java.util.List;
import org.apache.iceberg.catalog.TableIdentifier;
import org.immutables.value.Value;

@Value.Immutable
public abstract class CatalogMigrationResult {

  public abstract List<TableIdentifier> registeredTableIdentifiers();

  public abstract List<TableIdentifier> failedToRegisterTableIdentifiers();

  public abstract List<TableIdentifier> failedToDeleteTableIdentifiers();

  public void printSummary(
      PrintWriter printWriter,
      boolean deleteSourceCatalogTables,
      String sourceCatalogType,
      String targetCatalogType) {
    printWriter.println(String.format("%nSummary: "));
    if (!registeredTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- Successfully %s %d tables from %s catalog to %s catalog.",
              deleteSourceCatalogTables ? "migrated" : "registered",
              registeredTableIdentifiers().size(),
              sourceCatalogType,
              targetCatalogType));
    }
    if (!failedToRegisterTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- Failed to %s %d tables from %s catalog to %s catalog. "
                  + "Please check the `catalog_migration.log` file for the failure reason. "
                  + "%nFailed identifiers are written into `%s`. "
                  + "Retry with that file using `--identifiers-from-file` option "
                  + "if the failure is because of network/connection timeouts.",
              deleteSourceCatalogTables ? "migrate" : "register",
              failedToRegisterTableIdentifiers().size(),
              sourceCatalogType,
              targetCatalogType,
              FAILED_IDENTIFIERS_FILE));
    }
    if (!failedToDeleteTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- Failed to delete %d tables from %s catalog. "
                  + "Please check the `catalog_migration.log` file for the reason. "
                  + "%nFailed to delete identifiers are written into `%s`. ",
              failedToDeleteTableIdentifiers().size(),
              sourceCatalogType,
              FAILED_TO_DELETE_AT_SOURCE_FILE));
    }
  }

  public void printDetails(PrintWriter printWriter, boolean deleteSourceCatalogTables) {
    printWriter.println(String.format("%nDetails: "));
    if (!registeredTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- Successfully %s these tables:",
              deleteSourceCatalogTables ? "migrated" : "registered"));
      printWriter.println(registeredTableIdentifiers());
    }

    if (!failedToRegisterTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- Failed to %s these tables:", deleteSourceCatalogTables ? "migrate" : "register"));
      printWriter.println(failedToRegisterTableIdentifiers());
    }

    if (!failedToDeleteTableIdentifiers().isEmpty()) {
      printWriter.println("- [WARNING] Failed to delete these tables from source catalog:");
      printWriter.println(failedToDeleteTableIdentifiers());
    }
  }

  public void printDryRunResults(PrintWriter printWriter, boolean deleteSourceCatalogTables) {
    printWriter.println(String.format("%nSummary: "));
    if (registeredTableIdentifiers().isEmpty()) {
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
            registeredTableIdentifiers().size(),
            deleteSourceCatalogTables ? "migration" : "registration",
            DRY_RUN_FILE));

    printWriter.println(String.format("%nDetails: "));
    printWriter.println(
        String.format(
            "- Identified these tables for %s by dry-run:",
            deleteSourceCatalogTables ? "migration" : "registration"));
    printWriter.println(registeredTableIdentifiers());
  }
}
