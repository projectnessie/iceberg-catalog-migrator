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
package org.projectnessie.tools.catalog.migration.api;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.dynamodb.DynamoDbCatalog;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.dell.ecs.EcsCatalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.rest.RESTCatalog;

public final class CatalogMigrationUtil {

  private CatalogMigrationUtil() {}

  public enum CatalogType {
    CUSTOM,
    DYNAMODB,
    ECS,
    GLUE,
    HADOOP,
    HIVE,
    JDBC,
    NESSIE,
    REST
  }

  public static Catalog buildCatalog(
      Map<String, String> catalogProperties,
      CatalogType catalogType,
      String catalogName,
      String customCatalogImpl,
      Map<String, String> hadoopConf) {
    Preconditions.checkArgument(catalogProperties != null, "catalog properties is null");
    Preconditions.checkArgument(catalogType != null, "catalog type is null");
    Configuration catalogConf = new Configuration();
    if (hadoopConf != null) {
      hadoopConf.forEach(catalogConf::set);
    }
    if (catalogProperties.get("name") != null) {
      // Some catalogs like jdbc stores the catalog name from the client when the namespace or table
      // is created.
      // Hence, when accessing the tables from another client, catalog name should match.
      catalogName = catalogProperties.get("name");
    }
    return CatalogUtil.loadCatalog(
        catalogImpl(catalogType, customCatalogImpl), catalogName, catalogProperties, catalogConf);
  }

  private static String catalogImpl(CatalogType type, String customCatalogImpl) {
    switch (type) {
      case CUSTOM:
        Preconditions.checkArgument(
            customCatalogImpl != null && !customCatalogImpl.trim().isEmpty(),
            "Need to specify the fully qualified class name of the custom catalog impl");
        return customCatalogImpl;
      case DYNAMODB:
        return DynamoDbCatalog.class.getName();
      case ECS:
        return EcsCatalog.class.getName();
      case GLUE:
        return GlueCatalog.class.getName();
      case HADOOP:
        return HadoopCatalog.class.getName();
      case HIVE:
        return HiveCatalog.class.getName();
      case JDBC:
        return JdbcCatalog.class.getName();
      case NESSIE:
        return NessieCatalog.class.getName();
      case REST:
        return RESTCatalog.class.getName();
      default:
        throw new IllegalArgumentException("Unsupported type: " + type.name());
    }
  }
}
