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
import org.apache.iceberg.hive.HiveMetastoreExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ITHiveToNessieCatalogMigrator extends AbstractTestCatalogMigrator {

  @RegisterExtension
  public static final HiveMetastoreExtension HIVE_METASTORE_EXTENSION =
      HiveMetastoreExtension.builder().build();

  @BeforeAll
  protected static void setup() throws Exception {
    initializeSourceCatalog(
        CatalogMigrationUtil.CatalogType.HIVE,
        Collections.singletonMap(
            "uri", HIVE_METASTORE_EXTENSION.hiveConf().get("hive.metastore.uris")));
    initializeTargetCatalog(CatalogMigrationUtil.CatalogType.NESSIE, Collections.emptyMap());
  }

  @AfterAll
  protected static void tearDown() throws Exception {
    dropNamespaces();
  }
}
