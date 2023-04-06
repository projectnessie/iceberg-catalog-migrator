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
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.projectnessie.tools.catalog.migration.api.test.HiveMetaStoreRunner;

public class ITNessieToHiveCatalogMigrator extends AbstractTestCatalogMigrator {

  @BeforeAll
  protected static void setup() throws Exception {
    HiveMetaStoreRunner.startMetastore();

    initializeSourceCatalog(CatalogMigrationUtil.CatalogType.NESSIE, Collections.emptyMap());
    targetCatalog = HiveMetaStoreRunner.hiveCatalog();

    createNamespaces();
  }

  @AfterAll
  protected static void tearDown() throws Exception {
    dropNamespaces();
    HiveMetaStoreRunner.stopMetastore();
  }

  @Test
  public void testRegisterWithDefaultNamespace() {
    sourceCatalog.createTable(TBL, schema);

    CatalogMigrator catalogMigrator = catalogMigratorWithDefaultArgs(false);
    // should also include table from default namespace
    Set<TableIdentifier> matchingTableIdentifiers =
        catalogMigrator.getMatchingTableIdentifiers(null);
    Assertions.assertThat(matchingTableIdentifiers).contains(TBL);

    CatalogMigrationResult result =
        catalogMigrator.registerTables(matchingTableIdentifiers).result();
    // hive will not support default namespace (namespace with level = 0). Hence, register will
    // fail.
    Assertions.assertThat(result.registeredTableIdentifiers()).doesNotContain(TBL);
    Assertions.assertThat(result.failedToRegisterTableIdentifiers()).containsExactly(TBL);
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();
  }
}
