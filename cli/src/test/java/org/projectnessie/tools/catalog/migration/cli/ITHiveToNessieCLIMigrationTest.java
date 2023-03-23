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

import java.util.stream.IntStream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.tools.catalog.migration.api.test.HiveMetaStoreRunner;

public class ITHiveToNessieCLIMigrationTest extends AbstractCLIMigrationTest {

  protected static final int NESSIE_PORT = Integer.getInteger("quarkus.http.test-port", 19121);

  protected static String nessieUri = String.format("http://localhost:%d/api/v1", NESSIE_PORT);

  @BeforeAll
  protected static void setup() throws Exception {
    HiveMetaStoreRunner.startMetastore();
    sourceCatalogProperties =
        "warehouse="
            + warehouse1.toAbsolutePath()
            + ",uri="
            + HiveMetaStoreRunner.hiveCatalog().getConf().get("hive.metastore.uris");
    targetCatalogProperties =
        "uri=" + nessieUri + ",ref=main,warehouse=" + warehouse2.toAbsolutePath();

    sourceCatalog = HiveMetaStoreRunner.hiveCatalog();
    targetCatalog = createNessieCatalog(warehouse2.toAbsolutePath().toString(), nessieUri);

    sourceCatalogType = catalogType(sourceCatalog);
    targetCatalogType = catalogType(targetCatalog);

    createNamespaces();
  }

  @AfterAll
  protected static void tearDown() throws Exception {
    dropNamespaces();
    HiveMetaStoreRunner.stopMetastore();
  }

  // Executing migration of large number of tables for only one set of catalogs to save CI time.
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testRegisterLargeNumberOfTables(boolean deleteSourceTables) throws Exception {
    validateAssumptionForHadoopCatalogAsSource(deleteSourceTables);
    // additionally create 240 tables along with 4 tables created in beforeEach()
    IntStream.range(0, 240)
        .forEach(
            val ->
                sourceCatalog.createTable(
                    TableIdentifier.of(Namespace.of("foo"), "tblx" + val), schema));

    RunCLI run = runCLI(deleteSourceTables, registerAllTablesArgs());

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    String operation = deleteSourceTables ? "migration" : "registration";
    Assertions.assertThat(run.getOut())
        .contains(String.format("Identified 244 tables for %s.", operation));
    operation = deleteSourceTables ? "migrated" : "registered";
    Assertions.assertThat(run.getOut())
        .contains(
            String.format(
                "Summary: %nSuccessfully %s 244 tables from %s catalog to" + " %s catalog.",
                operation, sourceCatalogType, targetCatalogType));
    Assertions.assertThat(run.getOut())
        .contains(String.format("Details: %nSuccessfully %s these tables:%n", operation));

    operation = deleteSourceTables ? "migration" : "registration";
    // validate intermediate output
    Assertions.assertThat(run.getOut())
        .contains(String.format("Attempted %s for 100 tables out of 244 tables.", operation));
    Assertions.assertThat(run.getOut())
        .contains(String.format("Attempted %s for 200 tables out of 244 tables.", operation));

    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    targetCatalog.loadTable(TableIdentifier.parse("bar.tbl3")).refresh();

    Assertions.assertThat(targetCatalog.listTables(Namespace.of("foo"))).hasSize(242);
    Assertions.assertThat(targetCatalog.listTables(Namespace.of("bar")))
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("bar.tbl3"), TableIdentifier.parse("bar.tbl4"));
  }
}
