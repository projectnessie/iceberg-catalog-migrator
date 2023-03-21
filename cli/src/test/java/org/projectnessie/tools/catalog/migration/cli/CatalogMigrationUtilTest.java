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

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class CatalogMigrationUtilTest {

  protected static @TempDir Path logDir;

  @BeforeAll
  protected static void initLogDir() {
    System.setProperty("catalog.migration.log.dir", logDir.toAbsolutePath().toString());
  }

  static Stream<String> blankOrNullStrings() {
    return Stream.of("", " ", null);
  }

  @Order(0)
  @ParameterizedTest()
  @MethodSource("blankOrNullStrings")
  public void testCustomCatalogWithoutImpl(String impl) {
    Assertions.assertThatThrownBy(
            () ->
                CatalogMigrationUtil.buildCatalog(
                    Collections.emptyMap(),
                    CatalogMigrationUtil.CatalogType.CUSTOM,
                    "catalogName",
                    impl,
                    Collections.emptyMap()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Need to specify the fully qualified class name of the custom catalog impl");
  }

  @Order(1)
  @Test
  public void testInvalidArgs() {
    Assertions.assertThatThrownBy(
            () -> CatalogMigrationUtil.buildCatalog(null, null, null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("catalog properties is null");

    Assertions.assertThatThrownBy(
            () -> CatalogMigrationUtil.buildCatalog(Collections.emptyMap(), null, null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("catalog type is null");

    Assertions.assertThatThrownBy(
            () ->
                CatalogMigrationUtil.buildCatalog(
                    Collections.emptyMap(),
                    CatalogMigrationUtil.CatalogType.HIVE,
                    null,
                    null,
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("catalog name is null");

    Assertions.assertThatThrownBy(
            () ->
                CatalogMigrationUtil.buildCatalog(
                    Collections.emptyMap(), CatalogMigrationUtil.CatalogType.HIVE, " ", null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("catalog name is empty");

    Assertions.assertThatThrownBy(
            () ->
                CatalogMigrationUtil.buildCatalog(
                    Collections.emptyMap(),
                    CatalogMigrationUtil.CatalogType.CUSTOM,
                    "catalogName",
                    "abc",
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot initialize Catalog implementation abc: Cannot find constructor for interface");
  }

  @Test
  @Order(2)
  public void testBuildHadoopCatalog() {
    Map<String, String> properties = new HashMap<>();
    properties.put("warehouse", logDir.toAbsolutePath().toString());
    properties.put("type", "hadoop");

    Map<String, String> conf = new HashMap<>();
    conf.put("k1", "v1");

    Catalog catalog =
        CatalogMigrationUtil.buildCatalog(
            properties, CatalogMigrationUtil.CatalogType.HADOOP, "catalogName", null, conf);

    Assertions.assertThat(catalog).isInstanceOf(HadoopCatalog.class);
    Assertions.assertThat(catalog.name()).isEqualTo("catalogName");
    Assertions.assertThat(((HadoopCatalog) catalog).getConf().get("k1")).isEqualTo("v1");
    Schema schema =
        new Schema(
            Types.StructType.of(Types.NestedField.required(1, "id", Types.LongType.get()))
                .fields());
    Table table = catalog.createTable(TableIdentifier.parse("foo.tbl1"), schema);
    Assertions.assertThat(table.location()).contains(logDir.toAbsolutePath().toString());
    catalog.dropTable(TableIdentifier.parse("foo.tbl1"));
  }

  @Test
  @Order(3)
  public void testBuildNessieCatalog() {
    Map<String, String> properties = new HashMap<>();
    properties.put("warehouse", logDir.toAbsolutePath().toString());
    properties.put("ref", "main");
    properties.put("uri", "http://localhost:19120/api/v1");

    Catalog catalog =
        CatalogMigrationUtil.buildCatalog(
            properties, CatalogMigrationUtil.CatalogType.NESSIE, "catalogName", null, null);

    Assertions.assertThat(catalog).isInstanceOf(NessieCatalog.class);
    Assertions.assertThat(catalog.name()).isEqualTo("catalogName");
  }

  @Test
  @Order(4)
  public void testBuildHiveCatalog() {
    Map<String, String> properties = new HashMap<>();
    properties.put("warehouse", logDir.toAbsolutePath().toString());
    properties.put("type", "hive");
    properties.put("uri", "thrift://localhost:9083");

    Catalog catalog =
        CatalogMigrationUtil.buildCatalog(
            properties, CatalogMigrationUtil.CatalogType.HIVE, "catalogName", null, null);

    Assertions.assertThat(catalog).isInstanceOf(HiveCatalog.class);
    Assertions.assertThat(catalog.name()).isEqualTo("catalogName");
  }
}
