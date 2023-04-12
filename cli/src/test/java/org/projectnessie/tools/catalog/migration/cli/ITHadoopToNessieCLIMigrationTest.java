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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.projectnessie.tools.catalog.migration.api.CatalogMigrationUtil;

public class ITHadoopToNessieCLIMigrationTest extends AbstractCLIMigrationTest {

  @BeforeAll
  protected static void setup() {
    initializeSourceCatalog(CatalogMigrationUtil.CatalogType.HADOOP, Collections.emptyMap());
    initializeTargetCatalog(CatalogMigrationUtil.CatalogType.NESSIE, Collections.emptyMap());
  }

  @Test
  public void testRegisterLargeNumberOfTablesWithNestedNamespaces() throws Exception {
    List<Namespace> namespaceList =
        Arrays.asList(NS_A, NS_A_B, NS_A_B_C, NS_A_B_C_D, NS_A_B_C_D_E, NS_A_C);

    // additionally create 240 tables along with 4 tables created in beforeEach()
    namespaceList.forEach(
        namespace -> {
          ((SupportsNamespaces) sourceCatalog).createNamespace(namespace);
          IntStream.range(0, 40)
              .forEach(
                  val ->
                      sourceCatalog.createTable(
                          TableIdentifier.of(namespace, "tblx" + val), schema));
        });

    // register or migrate all the tables
    RunCLI run = runCLI(false, defaultArgs());

    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut())
        .contains("Identified 244 tables for registration.")
        .contains(
            String.format(
                "Summary: %nSuccessfully registered 244 tables from %s catalog to" + " %s catalog.",
                sourceCatalogType, targetCatalogType))
        .contains(String.format("Details: %nSuccessfully registered these tables:%n"))
        // validate intermediate output
        .contains("Attempted registration for 100 tables out of 244 tables.")
        .contains("Attempted registration for 200 tables out of 244 tables.")
        .contains("Attempted registration for 244 tables out of 244 tables.");

    // manually refreshing catalog due to missing refresh in Nessie catalog
    // https://github.com/apache/iceberg/pull/6789
    targetCatalog.loadTable(BAR_TBL3).refresh();

    Assertions.assertThat(targetCatalog.listTables(FOO))
        .containsExactlyInAnyOrder(FOO_TBL1, FOO_TBL2);
    Assertions.assertThat(targetCatalog.listTables(BAR))
        .containsExactlyInAnyOrder(BAR_TBL3, BAR_TBL4);

    Collections.reverse(namespaceList);
    namespaceList.forEach(
        namespace -> {
          List<TableIdentifier> identifiers = targetCatalog.listTables(namespace);

          // validate tables count in each namespace.
          Assertions.assertThat(identifiers).hasSize(40);

          identifiers.forEach(
              identifier -> {
                targetCatalog.dropTable(identifier);
                sourceCatalog.dropTable(identifier);
              });
          ((SupportsNamespaces) sourceCatalog).dropNamespace(namespace);
          ((SupportsNamespaces) targetCatalog).dropNamespace(namespace);
        });
  }
}
