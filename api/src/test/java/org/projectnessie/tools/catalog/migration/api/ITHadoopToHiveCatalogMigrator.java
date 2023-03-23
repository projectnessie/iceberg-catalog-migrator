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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.projectnessie.tools.catalog.migration.api.test.HiveMetaStoreRunner;

public class ITHadoopToHiveCatalogMigrator extends AbstractTestCatalogMigrator {

  @BeforeAll
  protected static void setup() throws Exception {
    HiveMetaStoreRunner.startMetastore();

    sourceCatalog = createHadoopCatalog(warehouse1.toAbsolutePath().toString(), "hadoop");
    targetCatalog = HiveMetaStoreRunner.hiveCatalog();

    createNamespaces();
  }

  @AfterAll
  protected static void tearDown() throws Exception {
    dropNamespaces();
    HiveMetaStoreRunner.stopMetastore();
  }

  @Test
  public void testRegisterWithNewNestedNamespace() {
    Namespace namespace = Namespace.of("a.b.c");
    TableIdentifier tableIdentifier = TableIdentifier.parse("a.b.c.tbl5");
    // create namespace "a.b.c" only in source catalog
    ((SupportsNamespaces) sourceCatalog).createNamespace(namespace);
    sourceCatalog.createTable(tableIdentifier, schema);

    CatalogMigrationResult result =
        catalogMigratorWithDefaultArgs(false)
            .registerTables(Collections.singletonList(tableIdentifier))
            .result();

    // hive catalog doesn't support multipart namespace. Hence, table should fail to register.
    Assertions.assertThat(result.registeredTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToRegisterTableIdentifiers())
        .containsExactly(tableIdentifier);
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    sourceCatalog.dropTable(tableIdentifier);
    ((SupportsNamespaces) sourceCatalog).dropNamespace(namespace);
  }
}
