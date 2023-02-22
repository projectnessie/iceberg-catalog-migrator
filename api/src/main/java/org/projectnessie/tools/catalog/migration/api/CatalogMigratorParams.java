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
import java.io.PrintWriter;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.immutables.value.Value;

@Value.Immutable
public interface CatalogMigratorParams {

  /** Source {@link Catalog} from which the tables are chosen. */
  Catalog sourceCatalog();

  /** Target {@link Catalog} to which the tables need to be migrated. */
  Catalog targetCatalog();

  /**
   * Optional List of {@link TableIdentifier} for the tables required to be migrated. If not
   * specified, all the tables would be migrated.
   */
  @Nullable
  List<TableIdentifier> tableIdentifiers();

  /**
   * Optional Regular expression pattern used to migrate only the tables whose identifiers match
   * this pattern. Can be provided instead of `tableIdentifiers`.
   */
  @Nullable
  String identifierRegex();

  /** To execute as dry run. */
  boolean isDryRun();

  /** Delete the table entries from source catalog after successful migration. */
  boolean deleteEntriesFromSourceCatalog();

  /** To print the regular updates on the console. */
  PrintWriter printWriter();

  /** optional path to store the result files. If null, uses present working directory. */
  @Nullable
  String outputDirPath();

  @Value.Check
  default void validate() {
    Preconditions.checkArgument(sourceCatalog() != null, "Invalid source catalog: null");
    Preconditions.checkArgument(targetCatalog() != null, "Invalid target catalog: null");
    Preconditions.checkArgument(
        !targetCatalog().equals(sourceCatalog()), "target catalog is same as source catalog");

    if (identifierRegex() != null && tableIdentifiers() != null && !tableIdentifiers().isEmpty()) {
      throw new IllegalArgumentException(
          "Both the identifiers list and identifierRegex is configured.");
    }
  }
}
