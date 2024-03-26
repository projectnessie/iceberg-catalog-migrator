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

import java.util.Collections;
import java.util.stream.IntStream;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveMetastoreExtension;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.tools.catalog.migration.api.CatalogMigrationUtil;

public class ITHiveToNessieCLIMigrationTest extends AbstractCLIMigrationTest {

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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterLargeNumberOfTables(boolean deleteSourceTables) throws Exception {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);

    String operation = deleteSourceTables ? "migration" : "registration";
    String operated = deleteSourceTables ? "migrated" : "registered";

    // additionally create 240 tables along with 4 tables created in beforeEach()
    IntStream.range(0, 240)
        .forEach(val -> sourceCatalog.createTable(TableIdentifier.of(FOO, "tblx" + val), schema));

    // register or migrate all the tables
    RunCLI run = runCLI(deleteSourceTables, defaultArgs());

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 244 tables for %s.", operation))
        .contains(
            String.format(
                "Summary: %nSuccessfully %s 244 tables from %s catalog to" + " %s catalog.",
                operated, sourceCatalogType, targetCatalogType))
        .contains(String.format("Details: %nSuccessfully %s these tables:%n", operated))
        // validate intermediate output
        .contains(String.format("Attempted %s for 100 tables out of 244 tables.", operation))
        .contains(String.format("Attempted %s for 200 tables out of 244 tables.", operation))
        .contains(String.format("Attempted %s for 244 tables out of 244 tables.", operation));

    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    targetCatalog.loadTable(BAR_TBL3).refresh();

    Assertions.assertThat(targetCatalog.listTables(FOO)).hasSize(242);
    Assertions.assertThat(targetCatalog.listTables(BAR))
        .containsExactlyInAnyOrder(BAR_TBL3, BAR_TBL4);
  }
}
