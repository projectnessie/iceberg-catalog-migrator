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

public class HadoopCLIMigrationTest extends AbstractCLIMigrationTest {

  @BeforeAll
  protected static void setup() {
    sourceCatalogProperties = "warehouse=" + warehouse1.toAbsolutePath() + ",type=hadoop";
    targetCatalogProperties = "warehouse=" + warehouse2.toAbsolutePath() + ",type=hadoop";

    sourceCatalog = createHadoopCatalog(warehouse1.toAbsolutePath().toString(), "sourceCatalog");
    targetCatalog = createHadoopCatalog(warehouse2.toAbsolutePath().toString(), "targetCatalog");

    sourceCatalogType = catalogType(sourceCatalog);
    targetCatalogType = catalogType(targetCatalog);

    createNamespaces();
  }

  @AfterAll
  protected static void tearDown() {
    dropNamespaces();
  }
}
