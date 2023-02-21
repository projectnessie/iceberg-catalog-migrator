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
package org.projectnessie.tools.catalog.migration;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.dynamodb.DynamoDbCatalog;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.dell.ecs.EcsCatalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

public abstract class AbstractTest {

  protected static @TempDir Path logDir;

  @BeforeAll
  protected static void initLogDir() {
    System.setProperty("catalog.migration.log.dir", logDir.toAbsolutePath().toString());
  }

  protected static Catalog catalog1;

  protected static Catalog catalog2;
  protected static final Schema schema =
      new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());

  protected static void createTables() {
    // two tables in 'foo' namespace
    catalog1.createTable(TableIdentifier.of(Namespace.of("foo"), "tbl1"), schema);
    catalog1.createTable(TableIdentifier.of(Namespace.of("foo"), "tbl2"), schema);
    // two tables in 'bar' namespace
    catalog1.createTable(TableIdentifier.of(Namespace.of("bar"), "tbl3"), schema);
    catalog1.createTable(TableIdentifier.of(Namespace.of("bar"), "tbl4"), schema);
  }

  protected static void createNamespaces() {
    ((SupportsNamespaces) catalog1).createNamespace(Namespace.of("foo"), Collections.emptyMap());
    ((SupportsNamespaces) catalog1).createNamespace(Namespace.of("bar"), Collections.emptyMap());

    ((SupportsNamespaces) catalog2).createNamespace(Namespace.of("foo"), Collections.emptyMap());
    ((SupportsNamespaces) catalog2).createNamespace(Namespace.of("bar"), Collections.emptyMap());
  }

  protected static void dropNamespaces() {
    ((SupportsNamespaces) catalog1).dropNamespace(Namespace.of("foo"));
    ((SupportsNamespaces) catalog1).dropNamespace(Namespace.of("bar"));

    ((SupportsNamespaces) catalog2).dropNamespace(Namespace.of("foo"));
    ((SupportsNamespaces) catalog2).dropNamespace(Namespace.of("bar"));
  }

  protected static void deleteFileIfExists(Path filePath) throws IOException {
    if (Files.exists(filePath)) {
      Files.delete(filePath);
    }
  }

  protected static String catalogType(Catalog catalog) {
    if (catalog instanceof DynamoDbCatalog) {
      return CatalogMigrationCLI.CatalogType.DYNAMODB.name();
    } else if (catalog instanceof EcsCatalog) {
      return CatalogMigrationCLI.CatalogType.ECS.name();
    } else if (catalog instanceof GlueCatalog) {
      return CatalogMigrationCLI.CatalogType.GLUE.name();
    } else if (catalog instanceof HadoopCatalog) {
      return CatalogMigrationCLI.CatalogType.HADOOP.name();
    } else if (catalog instanceof HiveCatalog) {
      return CatalogMigrationCLI.CatalogType.HIVE.name();
    } else if (catalog instanceof JdbcCatalog) {
      return CatalogMigrationCLI.CatalogType.JDBC.name();
    } else if (catalog instanceof NessieCatalog) {
      return CatalogMigrationCLI.CatalogType.NESSIE.name();
    } else if (catalog instanceof RESTCatalog) {
      return CatalogMigrationCLI.CatalogType.REST.name();
    } else {
      return CatalogMigrationCLI.CatalogType.CUSTOM.name();
    }
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

  protected static void dropTables() {
    Arrays.asList(Namespace.of("foo"), Namespace.of("bar"))
        .forEach(
            namespace -> {
              catalog1.listTables(namespace).forEach(catalog1::dropTable);
              catalog2.listTables(namespace).forEach(catalog2::dropTable);
            });
  }
}
