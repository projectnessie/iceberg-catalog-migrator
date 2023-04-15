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

import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.projectnessie.tools.catalog.migration.api.test.AbstractTest;

public class CatalogMigratorParamsTest extends AbstractTest {

  @Test
  public void testInvalidArgs() {
    sourceCatalog =
        CatalogUtil.loadCatalog(
            HadoopCatalog.class.getName(),
            "source",
            hadoopCatalogProperties(true),
            new Configuration());
    targetCatalog =
        CatalogUtil.loadCatalog(
            HadoopCatalog.class.getName(),
            "target",
            hadoopCatalogProperties(true),
            new Configuration());

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
                    .targetCatalog(targetCatalog)
                    .deleteEntriesFromSourceCatalog(false)
                    .build()
                    .registerTable(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Identifier is null");

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
