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
import org.apache.iceberg.catalog.Catalog;
import org.immutables.value.Value;

@Value.Immutable
public interface CatalogMigratorParams {

  /** Source {@link Catalog} from which the tables are chosen. */
  Catalog sourceCatalog();

  /** Target {@link Catalog} to which the tables need to be migrated. */
  Catalog targetCatalog();

  /** Delete the table entries from source catalog after successful migration. */
  boolean deleteEntriesFromSourceCatalog();

  @Value.Check
  default void validate() {
    Preconditions.checkArgument(
        !targetCatalog().equals(sourceCatalog()), "target catalog is same as source catalog");
  }
}
