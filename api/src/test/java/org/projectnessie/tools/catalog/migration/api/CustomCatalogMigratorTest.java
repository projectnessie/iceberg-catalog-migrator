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
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.tools.catalog.migration.api.test.AbstractTest;

public class CustomCatalogMigratorTest extends AbstractTest {

  protected static @TempDir Path warehouse1;
  protected static @TempDir Path warehouse2;

  @BeforeAll
  protected static void setup() {
    sourceCatalog = createCustomCatalog(warehouse1.toAbsolutePath().toString(), "sourceCatalog");
    targetCatalog = createCustomCatalog(warehouse2.toAbsolutePath().toString(), "targetCatalog");
  }

  @BeforeEach
  protected void beforeEach() {
    createTables();
  }

  @AfterEach
  protected void afterEach() {
    dropTables();
  }

  @Test
  public void testRegister() {
    CatalogMigrator catalogMigrator =
        ImmutableCatalogMigrator.builder()
            .sourceCatalog(sourceCatalog)
            .targetCatalog(targetCatalog)
            .deleteEntriesFromSourceCatalog(false)
            .build();
    // should fail to register as catalog doesn't support register table operations.
    catalogMigrator.getMatchingTableIdentifiers(null).forEach(catalogMigrator::registerTable);
    CatalogMigrationResult result = catalogMigrator.result();
    Assertions.assertThat(result.registeredTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToRegisterTableIdentifiers())
        .containsExactlyInAnyOrder(FOO_TBL1, FOO_TBL2, BAR_TBL3, BAR_TBL4);
  }

  private static Catalog createCustomCatalog(String warehousePath, String name) {

    class TestCatalog extends HadoopCatalog {
      @Override
      public Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
        throw new UnsupportedOperationException("This catalog doesn't support register table");
      }
    }

    Map<String, String> properties = new HashMap<>();
    properties.put("warehouse", warehousePath);
    TestCatalog testCatalog = new TestCatalog();
    testCatalog.setConf(new Configuration());
    testCatalog.initialize(name, properties);
    return testCatalog;
  }
}
