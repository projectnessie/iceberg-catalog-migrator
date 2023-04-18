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
import java.util.List;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class UnsupportedNamespaceTest {

  protected static @TempDir Path tempDir;

  @BeforeAll
  protected static void initLogDir() {
    System.setProperty("catalog.migration.log.dir", tempDir.toAbsolutePath().toString());
  }

  @Test
  public void testUnsupportedNamespace() {

    class TestCatalog extends BaseMetastoreCatalog {
      // doesn't support namespaces
      @Override
      protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
        return null;
      }

      @Override
      protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        return null;
      }

      @Override
      public List<TableIdentifier> listTables(Namespace namespace) {
        return null;
      }

      @Override
      public boolean dropTable(TableIdentifier identifier, boolean purge) {
        return false;
      }

      @Override
      public void renameTable(TableIdentifier from, TableIdentifier to) {}
    }

    Catalog sourceCatalog = new TestCatalog();
    Catalog targetCatalog = new TestCatalog();

    Assertions.assertThatThrownBy(
            () ->
                ImmutableCatalogMigrator.builder()
                    .sourceCatalog(sourceCatalog)
                    .targetCatalog(targetCatalog)
                    .deleteEntriesFromSourceCatalog(false)
                    .build())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining(
            "target catalog TestCatalog{} doesn't implement SupportsNamespaces to create missing namespaces.");

    Assertions.assertThatThrownBy(
            () ->
                ImmutableCatalogMigrator.builder()
                    .sourceCatalog(sourceCatalog)
                    .targetCatalog(new HadoopCatalog())
                    .deleteEntriesFromSourceCatalog(false)
                    .build())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining(
            "source catalog TestCatalog{} doesn't implement SupportsNamespaces to list all namespaces.");
  }
}
