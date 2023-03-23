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
package org.projectnessie.tools.catalog.migration.api.test;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

public abstract class AbstractTest {

  protected static @TempDir Path logDir;

  private static final List<Namespace> namespaceList =
      Arrays.asList(Namespace.of("foo"), Namespace.of("bar"), Namespace.of("db1"));

  @BeforeAll
  protected static void initLogDir() {
    System.setProperty("catalog.migration.log.dir", logDir.toAbsolutePath().toString());
  }

  protected static Catalog sourceCatalog;

  protected static Catalog targetCatalog;

  protected static final Schema schema =
      new Schema(
          Types.StructType.of(Types.NestedField.required(1, "id", Types.LongType.get())).fields());

  protected void validateAssumptionForHadoopCatalogAsSource(boolean deleteSourceTables) {
    Assumptions.assumeFalse(
        deleteSourceTables && sourceCatalog instanceof HadoopCatalog,
        "deleting source tables is unsupported for HadoopCatalog");
  }

  protected static void createNamespaces() {
    namespaceList.forEach(
        namespace -> ((SupportsNamespaces) sourceCatalog).createNamespace(namespace));
    // don't create "db1" namespace in targetCatalog
    namespaceList
        .subList(0, 2)
        .forEach(namespace -> ((SupportsNamespaces) targetCatalog).createNamespace(namespace));
  }

  protected static void dropNamespaces() {
    Stream.of(sourceCatalog, targetCatalog)
        .map(catalog -> (SupportsNamespaces) catalog)
        .forEach(
            catalog ->
                namespaceList.stream()
                    .filter(catalog::namespaceExists)
                    .forEach(catalog::dropNamespace));
  }

  protected static void createTables() {
    // two tables in 'foo' namespace
    sourceCatalog.createTable(TableIdentifier.of(Namespace.of("foo"), "tbl1"), schema);
    sourceCatalog.createTable(TableIdentifier.of(Namespace.of("foo"), "tbl2"), schema);
    // two tables in 'bar' namespace
    sourceCatalog.createTable(TableIdentifier.of(Namespace.of("bar"), "tbl3"), schema);
    sourceCatalog.createTable(TableIdentifier.of(Namespace.of("bar"), "tbl4"), schema);
  }

  protected static void dropTables() {
    Stream.of(sourceCatalog, targetCatalog)
        .forEach(
            catalog ->
                namespaceList.stream()
                    .filter(namespace -> ((SupportsNamespaces) catalog).namespaceExists(namespace))
                    .forEach(
                        namespace -> catalog.listTables(namespace).forEach(catalog::dropTable)));
  }

  protected static Catalog createHadoopCatalog(String warehousePath, String name) {
    Map<String, String> properties = new HashMap<>();
    properties.put("warehouse", warehousePath);
    return CatalogUtil.loadCatalog(
        HadoopCatalog.class.getName(), name, properties, new Configuration());
  }

  protected static Catalog createNessieCatalog(String warehousePath, String uri) {
    Map<String, String> properties = new HashMap<>();
    properties.put("warehouse", warehousePath);
    properties.put("ref", "main");
    properties.put("uri", uri);
    return CatalogUtil.loadCatalog(
        NessieCatalog.class.getName(), "nessie", properties, new Configuration());
  }
}
