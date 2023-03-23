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
package org.projectnessie.tools.catalog.migration.cli;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.projectnessie.tools.catalog.migration.api.test.HiveMetaStoreRunner;

public class ITNessieToHiveCLIMigrationTest extends AbstractCLIMigrationTest {

  protected static final int NESSIE_PORT = Integer.getInteger("quarkus.http.test-port", 19121);

  protected static String nessieUri = String.format("http://localhost:%d/api/v1", NESSIE_PORT);

  @BeforeAll
  protected static void setup() throws Exception {
    HiveMetaStoreRunner.startMetastore();
    sourceCatalogProperties = "uri=" + nessieUri + ",ref=main,warehouse=" + warehouse1;
    targetCatalogProperties =
        "warehouse="
            + warehouse2
            + ",uri="
            + HiveMetaStoreRunner.hiveCatalog().getConf().get("hive.metastore.uris");

    sourceCatalog = createNessieCatalog(warehouse1.toAbsolutePath().toString(), nessieUri);
    targetCatalog = HiveMetaStoreRunner.hiveCatalog();

    sourceCatalogType = catalogType(sourceCatalog);
    targetCatalogType = catalogType(targetCatalog);

    createNamespaces();
  }

  @AfterAll
  protected static void tearDown() throws Exception {
    dropNamespaces();
    HiveMetaStoreRunner.stopMetastore();
  }
}
