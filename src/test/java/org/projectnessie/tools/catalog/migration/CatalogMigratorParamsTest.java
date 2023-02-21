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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class CatalogMigratorParamsTest {

  @Test
  public void testInvalidArgs() throws IOException {
    Catalog catalog1 = new HadoopCatalog();
    Catalog catalog2 = new HadoopCatalog();

    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);

    try {
      Assertions.assertThatThrownBy(
              () ->
                  ImmutableCatalogMigratorParams.builder()
                      .sourceCatalog(catalog2) // source-catalog is same as target catalog
                      .targetCatalog(catalog2)
                      .isDryRun(false)
                      .printWriter(printWriter)
                      .outputDirPath("temp")
                      .deleteEntriesFromSourceCatalog(true)
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("target catalog is same as source catalog");

      Assertions.assertThatThrownBy(
              () ->
                  ImmutableCatalogMigratorParams.builder()
                      .sourceCatalog(catalog1)
                      .targetCatalog(catalog2)
                      .isDryRun(false)
                      .printWriter(printWriter)
                      .deleteEntriesFromSourceCatalog(true)
                      .identifierRegex(".*")
                      .tableIdentifiers(Collections.singletonList(TableIdentifier.parse("foo.abc")))
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Both the identifiers list and identifierRegex is configured.");

      Assertions.assertThatThrownBy(
              () ->
                  ImmutableCatalogMigratorParams.builder()
                      .sourceCatalog(catalog1)
                      .targetCatalog(null) // target-catalog is null
                      .isDryRun(false)
                      .printWriter(printWriter)
                      .outputDirPath("temp")
                      .deleteEntriesFromSourceCatalog(true)
                      .build())
          .isInstanceOf(NullPointerException.class)
          .hasMessageContaining("targetCatalog");

      Assertions.assertThatThrownBy(
              () ->
                  ImmutableCatalogMigratorParams.builder()
                      .sourceCatalog(null) // source-catalog is null
                      .targetCatalog(catalog2)
                      .isDryRun(false)
                      .printWriter(printWriter)
                      .outputDirPath("temp")
                      .deleteEntriesFromSourceCatalog(true)
                      .build())
          .isInstanceOf(NullPointerException.class)
          .hasMessageContaining("sourceCatalog");

      Assertions.assertThatThrownBy(
              () ->
                  ImmutableCatalogMigratorParams.builder()
                      .sourceCatalog(catalog1)
                      .targetCatalog(catalog2)
                      .isDryRun(false)
                      .printWriter(null)
                      .deleteEntriesFromSourceCatalog(true)
                      .build())
          .isInstanceOf(NullPointerException.class)
          .hasMessageContaining("printWriter");
    } finally {
      stringWriter.close();
      printWriter.close();
    }
  }
}
