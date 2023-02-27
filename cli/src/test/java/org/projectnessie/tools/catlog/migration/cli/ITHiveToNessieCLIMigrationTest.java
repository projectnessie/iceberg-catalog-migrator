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
package org.projectnessie.tools.catlog.migration.cli;

import static org.projectnessie.tools.catalog.migration.api.CatalogMigrator.DRY_RUN_FILE;
import static org.projectnessie.tools.catalog.migration.api.CatalogMigrator.FAILED_IDENTIFIERS_FILE;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.projectnessie.tools.catalog.migration.api.test.HiveMetaStoreRunner;

public class ITHiveToNessieCLIMigrationTest extends AbstractCLIMigrationTest {

  protected static final int NESSIE_PORT = Integer.getInteger("quarkus.http.test-port", 19121);

  protected static String nessieUri = String.format("http://localhost:%d/api/v1", NESSIE_PORT);

  @BeforeAll
  protected static void setup() throws Exception {
    HiveMetaStoreRunner.startMetastore();
    dryRunFile = outputDir.resolve(DRY_RUN_FILE);
    failedIdentifiersFile = outputDir.resolve(FAILED_IDENTIFIERS_FILE);
    sourceCatalogProperties =
        "warehouse="
            + warehouse1.toAbsolutePath()
            + ",uri="
            + HiveMetaStoreRunner.hiveCatalog().getConf().get("hive.metastore.uris");
    targetCatalogProperties =
        "uri=" + nessieUri + ",ref=main,warehouse=" + warehouse2.toAbsolutePath();

    catalog1 = HiveMetaStoreRunner.hiveCatalog();
    catalog2 = createNessieCatalog(warehouse2.toAbsolutePath().toString(), nessieUri);

    sourceCatalogType = catalogType(catalog1);
    targetCatalogType = catalogType(catalog2);

    createNamespaces();
  }

  @AfterAll
  protected static void tearDown() throws Exception {
    dropNamespaces();
    HiveMetaStoreRunner.stopMetastore();
  }

  // disable large table test for IT to save CI time. It will be executed only for UT.
  @Override
  @Disabled
  public void testRegisterLargeNumberOfTables(boolean deleteSourceTables) throws Exception {
    super.testRegisterLargeNumberOfTables(deleteSourceTables);
  }
}
