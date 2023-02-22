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

public class HadoopCLIMigrationTest extends AbstractCLIMigrationTest {

  @BeforeAll
  protected static void setup() {
    dryRunFile = outputDir.resolve(DRY_RUN_FILE);
    failedIdentifiersFile = outputDir.resolve(FAILED_IDENTIFIERS_FILE);
    sourceCatalogProperties = "warehouse=" + warehouse1.toAbsolutePath() + ",type=hadoop";
    targetCatalogProperties = "warehouse=" + warehouse2.toAbsolutePath() + ",type=hadoop";

    catalog1 = createHadoopCatalog(warehouse1.toAbsolutePath().toString(), "catalog1");
    catalog2 = createHadoopCatalog(warehouse2.toAbsolutePath().toString(), "catalog2");

    sourceCatalogType = catalogType(catalog1);
    targetCatalogType = catalogType(catalog2);

    createNamespaces();
  }

  @AfterAll
  protected static void tearDown() {
    dropNamespaces();
  }
}
