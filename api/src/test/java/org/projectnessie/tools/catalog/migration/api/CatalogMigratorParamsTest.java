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

import java.nio.file.Path;
import java.util.Collections;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class CatalogMigratorParamsTest {

  protected static @TempDir Path logDir;

  @BeforeAll
  protected static void initLogDir() {
    System.setProperty("catalog.migration.log.dir", logDir.toAbsolutePath().toString());
  }

  @Test
  public void testInvalidArgs() {
    Catalog catalog1 = new HadoopCatalog();
    Catalog catalog2 = new HadoopCatalog();

    Assertions.assertThatThrownBy(
            () ->
                ImmutableCatalogMigrator.builder()
                    .sourceCatalog(catalog2) // source-catalog is same as target catalog
                    .targetCatalog(catalog2)
                    .deleteEntriesFromSourceCatalog(true)
                    .build()
                    .registerTables(Collections.singletonList(TableIdentifier.parse("foo.abc"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("target catalog is same as source catalog");

    Assertions.assertThatThrownBy(
            () ->
                ImmutableCatalogMigrator.builder()
                    .sourceCatalog(catalog1)
                    .targetCatalog(catalog2)
                    .deleteEntriesFromSourceCatalog(true)
                    .build()
                    .registerTables(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Identifiers list is null");

    Assertions.assertThatThrownBy(
            () ->
                ImmutableCatalogMigrator.builder()
                    .sourceCatalog(catalog1)
                    .targetCatalog(null) // target-catalog is null
                    .deleteEntriesFromSourceCatalog(true)
                    .build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("targetCatalog");

    Assertions.assertThatThrownBy(
            () ->
                ImmutableCatalogMigrator.builder()
                    .sourceCatalog(null) // source-catalog is null
                    .targetCatalog(catalog2)
                    .deleteEntriesFromSourceCatalog(true)
                    .build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("sourceCatalog");
  }
}
