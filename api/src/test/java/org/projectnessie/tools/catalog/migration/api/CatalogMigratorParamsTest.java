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
    Catalog sourceCatalog = new HadoopCatalog();
    Catalog targetCatalog = new HadoopCatalog();

    Assertions.assertThatThrownBy(
            () ->
                ImmutableCatalogMigrator.builder()
                    .sourceCatalog(targetCatalog) // source-catalog is same as target catalog
                    .targetCatalog(targetCatalog)
                    .deleteEntriesFromSourceCatalog(true)
                    .build()
                    .registerTables(Collections.singletonList(TableIdentifier.parse("foo.abc"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("target catalog is same as source catalog");

    Assertions.assertThatThrownBy(
            () ->
                ImmutableCatalogMigrator.builder()
                    .sourceCatalog(sourceCatalog)
                    .targetCatalog(targetCatalog)
                    .deleteEntriesFromSourceCatalog(false)
                    .build()
                    .registerTables(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Identifiers list is null");

    Assertions.assertThatThrownBy(
            () ->
                ImmutableCatalogMigrator.builder()
                    .sourceCatalog(sourceCatalog)
                    .targetCatalog(null) // target-catalog is null
                    .deleteEntriesFromSourceCatalog(true)
                    .build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("targetCatalog");

    Assertions.assertThatThrownBy(
            () ->
                ImmutableCatalogMigrator.builder()
                    .sourceCatalog(null) // source-catalog is null
                    .targetCatalog(targetCatalog)
                    .deleteEntriesFromSourceCatalog(true)
                    .build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("sourceCatalog");

    // test source catalog as hadoop with `deleteEntriesFromSourceCatalog` as true.
    Assertions.assertThatThrownBy(
            () ->
                ImmutableCatalogMigrator.builder()
                    .sourceCatalog(sourceCatalog)
                    .targetCatalog(targetCatalog)
                    .deleteEntriesFromSourceCatalog(true)
                    .build()
                    .registerTables(Collections.emptyList()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining(
            "Source catalog is a Hadoop catalog and it doesn't support deleting the table entries just from the catalog. "
                + "Please configure `deleteEntriesFromSourceCatalog` as `false`");
  }
}
