/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.tool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.dynamodb.DynamoDbCatalog;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.dell.ecs.EcsCatalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import picocli.CommandLine;

@CommandLine.Command(name = "register", mixinStandardHelpOptions = true, version = "1.0",
    sortOptions = false,
    description = "\nBulk register the iceberg tables from source catalog to target catalog.\n")
public class CatalogMigrationCLI implements Callable<Integer> {

  @CommandLine.Option(
      names = "--source-catalog-hadoop-conf",
      split = ",",
      description =
          "source catalog Hadoop configuration, required when using an Iceberg FileIO.")
  Map<String, String> sourceHadoopConf = new HashMap<>();
  @CommandLine.Option(
      names = "--target-catalog-hadoop-conf",
      split = ",",
      description =
          "target catalog Hadoop configuration, required when using an Iceberg FileIO.")
  Map<String, String> targetHadoopConf = new HashMap<>();
  @CommandLine.Option(
      names = {"-I", "--identifiers"},
      split = ",",
      description =
          "selective list of identifiers to register. If not specified, all the tables will be registered")
  List<String> identifiers = new ArrayList<>();
  @CommandLine.Option(
      names = {"-T", "--thread-pool-size"},
      defaultValue = "0",
      description = "Size of the thread pool used for register tables")
  int maxThreadPoolSize;
  @CommandLine.Option(
      names = {"--source-custom-catalog-impl"},
      description =
          "fully qualified class name of the custom catalog implementation of the source catalog")
  String sourceCustomCatalogImpl;
  @CommandLine.Option(
      names = {"--target-custom-catalog-impl"},
      description =
          "fully qualified class name of the custom catalog implementation of the target catalog")
  String targetCustomCatalogImpl;
  @CommandLine.Parameters(index = "0", description = "source catalog type. " +
      "Can be one of these [CUSTOM, DYNAMODB, ECS, GLUE, HADOOP, HIVE, JDBC, NESSIE, REST]")
  private CatalogType sourceCatalogType;
  @CommandLine.Parameters(index = "1", split = ",", description = "source catalog properties")
  private Map<String, String> sourceCatalogProperties;
  @CommandLine.Parameters(index = "2", description = "target catalog type. " +
      "Can be one of these [CUSTOM, DYNAMODB, ECS, GLUE, HADOOP, HIVE, JDBC, NESSIE, REST]")
  private CatalogType targetCatalogType;
  @CommandLine.Parameters(index = "3", split = ",", description = "target catalog properties")
  private Map<String, String> targetCatalogProperties;
  @CommandLine.Option(
      names = {"-M", "--migrate"},
      description = "Deletes the tables entry from source catalog after the migration.")
  private boolean migrate;

  public static void main(String... args) {
    CommandLine commandLine = new CommandLine(new CatalogMigrationCLI());
    commandLine.setUsageHelpWidth(150);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() {
    Configuration sourceCatalogConf = new Configuration();
    if (sourceHadoopConf != null && !sourceHadoopConf.isEmpty()) {
      sourceHadoopConf.forEach(sourceCatalogConf::set);
    }
    Catalog sourceCatalog = CatalogUtil.loadCatalog(Objects.requireNonNull(catalogImpl(
            sourceCatalogType,
            sourceCustomCatalogImpl)), "sourceCatalog",
        sourceCatalogProperties,
        sourceCatalogConf);

    Configuration targetCatalogConf = new Configuration();
    if (targetHadoopConf != null && !targetHadoopConf.isEmpty()) {
      targetHadoopConf.forEach(targetCatalogConf::set);
    }
    Catalog targetCatalog = CatalogUtil.loadCatalog(Objects.requireNonNull(catalogImpl(
            targetCatalogType,
            targetCustomCatalogImpl)), "targetCatalog",
        targetCatalogProperties,
        targetCatalogConf);

    List<TableIdentifier> tableIdentifiers =
        identifiers == null ? null : identifiers.stream().map(TableIdentifier::parse).collect(Collectors.toList());
    Collection<TableIdentifier> result;
    if (migrate) {
      result = CatalogMigrateUtil.migrateTables(tableIdentifiers, sourceCatalog, targetCatalog, maxThreadPoolSize);
    } else {
      result = CatalogMigrateUtil.registerTables(tableIdentifiers, sourceCatalog, targetCatalog, maxThreadPoolSize);
    }
    System.out.println();
    System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>> ");
    System.out.printf("Successfully %s %d tables from %s catalog to %s catalog: ", migrate ? "migrated" :
            "registered",
        result.size(), sourceCatalogType.name(), targetCatalogType.name());
    System.out.println();
    result.forEach(System.out::println);

    return 0;
  }

  private String catalogImpl(CatalogType type, String customCatalogImpl) {
    switch (type) {
      case CUSTOM:
        if (customCatalogImpl == null || customCatalogImpl.isEmpty()) {
          throw new IllegalArgumentException("Need to specify the fully qualified class name of the custom catalog " +
              "impl");
        }
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
    }
    return null;
  }

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
}
