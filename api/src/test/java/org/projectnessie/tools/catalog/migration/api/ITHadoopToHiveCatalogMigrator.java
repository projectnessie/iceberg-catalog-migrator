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
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveMetastoreExtension;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ITHadoopToHiveCatalogMigrator extends AbstractTestCatalogMigrator {

  @RegisterExtension
  public static final HiveMetastoreExtension HIVE_METASTORE_EXTENSION =
      HiveMetastoreExtension.builder().build();

  @BeforeAll
  protected static void setup() throws Exception {
    initializeSourceCatalog(CatalogMigrationUtil.CatalogType.HADOOP, Collections.emptyMap());
    initializeTargetCatalog(
        CatalogMigrationUtil.CatalogType.HIVE,
        Collections.singletonMap(
            "uri", HIVE_METASTORE_EXTENSION.hiveConf().get("hive.metastore.uris")));
  }

  @AfterAll
  protected static void tearDown() throws Exception {
    dropNamespaces();
  }

  @Test
  public void testRegisterWithNewNestedNamespace() {
    TableIdentifier tableIdentifier = TableIdentifier.of(NS_A_B_C, "tbl5");
    // create namespace "a.b.c" only in source catalog
    ((SupportsNamespaces) sourceCatalog).createNamespace(NS_A_B_C);
    sourceCatalog.createTable(tableIdentifier, schema);

    CatalogMigrationResult result =
        catalogMigratorWithDefaultArgs(false).registerTable(tableIdentifier).result();

    // hive catalog doesn't support multipart namespace. Hence, table should fail to register.
    Assertions.assertThat(result.registeredTableIdentifiers()).isEmpty();
    Assertions.assertThat(result.failedToRegisterTableIdentifiers())
        .containsExactly(tableIdentifier);
    Assertions.assertThat(result.failedToDeleteTableIdentifiers()).isEmpty();

    sourceCatalog.dropTable(tableIdentifier);
    ((SupportsNamespaces) sourceCatalog).dropNamespace(NS_A_B_C);
  }
}
