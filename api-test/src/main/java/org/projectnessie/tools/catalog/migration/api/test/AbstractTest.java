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

  protected static Catalog catalog1;

  protected static Catalog catalog2;
  protected static final Schema schema =
      new Schema(
          Types.StructType.of(Types.NestedField.required(1, "id", Types.LongType.get())).fields());

  protected static void createNamespaces() {
    namespaceList.forEach(namespace -> ((SupportsNamespaces) catalog1).createNamespace(namespace));
    // don't create "db1" namespace in catalog2
    namespaceList
        .subList(0, 2)
        .forEach(namespace -> ((SupportsNamespaces) catalog2).createNamespace(namespace));
  }

  protected static void dropNamespaces() {
    Stream.of(catalog1, catalog2)
        .map(catalog -> (SupportsNamespaces) catalog)
        .forEach(
            catalog ->
                namespaceList.stream()
                    .filter(catalog::namespaceExists)
                    .forEach(catalog::dropNamespace));
  }

  protected static void createTables() {
    // two tables in 'foo' namespace
    catalog1.createTable(TableIdentifier.of(Namespace.of("foo"), "tbl1"), schema);
    catalog1.createTable(TableIdentifier.of(Namespace.of("foo"), "tbl2"), schema);
    // two tables in 'bar' namespace
    catalog1.createTable(TableIdentifier.of(Namespace.of("bar"), "tbl3"), schema);
    catalog1.createTable(TableIdentifier.of(Namespace.of("bar"), "tbl4"), schema);
  }

  protected static void dropTables() {
    Stream.of(catalog1, catalog2)
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
    properties.put("type", "hadoop");
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
