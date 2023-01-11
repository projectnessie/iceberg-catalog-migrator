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

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
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

@CommandLine.Command(
    name = "register",
    mixinStandardHelpOptions = true,
    versionProvider = CLIVersionProvider.class,
    // As both source and target catalog has similar configurations,
    // documentation is easy to read if the target and source property is one after another instead
    // of sorted.
    sortOptions = false,
    description =
        "\nBulk register the iceberg tables from source catalog to target catalog without data copy.\n")
public class CatalogMigrationCLI implements Callable<Integer> {
  @CommandLine.Spec CommandLine.Model.CommandSpec commandSpec;

  @CommandLine.Option(
      names = "--source-catalog-hadoop-conf",
      split = ",",
      description =
          "optional source catalog Hadoop configurations (like fs.s3a.secret.key, fs.s3a.access.key) required when "
              + "using an Iceberg FileIO.")
  Map<String, String> sourceHadoopConf = new HashMap<>();

  @CommandLine.Option(
      names = "--target-catalog-hadoop-conf",
      split = ",",
      description =
          "optional target catalog Hadoop configurations (like fs.s3a.secret.key, fs.s3a.access.key) required when "
              + "using an Iceberg FileIO.")
  Map<String, String> targetHadoopConf = new HashMap<>();

  @CommandLine.Option(
      names = {"-I", "--identifiers"},
      split = ",",
      description =
          "optional selective list of identifiers to register. If not specified, all the tables will be registered."
              + "Use this when there are few identifiers needs to be registered. For large number of identifiers, use "
              + "`--identifiers-from-file` option.")
  List<String> identifiers = new ArrayList<>();

  @CommandLine.Option(
      names = {"--identifiers-from-file"},
      description =
          "optional text file path that contains list of table identifiers (one per line) to register. Should not be "
              + "used with `--identifiers` option.")
  String identifiersFromFile;

  @CommandLine.Option(
      names = {"-T", "--thread-pool-size"},
      defaultValue = "0",
      description =
          "optional size of the thread pool used for register tables. Tables are migrated sequentially if "
              + "not specified.")
  int maxThreadPoolSize;

  @CommandLine.Option(
      names = {"--source-custom-catalog-impl"},
      description =
          "optional fully qualified class name of the custom catalog implementation of the source catalog. Required "
              + "when the catalog type is CUSTOM.")
  String sourceCustomCatalogImpl;

  @CommandLine.Option(
      names = {"--target-custom-catalog-impl"},
      description =
          "optional fully qualified class name of the custom catalog implementation of the target catalog. Required "
              + "when the catalog type is CUSTOM.")
  String targetCustomCatalogImpl;

  @CommandLine.Option(
      names = {"--delete-source-tables"},
      description =
          "Optional configuration to delete the tables entry from source catalog after successfully registering it "
              + "to target catalog.")
  private boolean deleteSourceCatalogTables;

  @CommandLine.Parameters(
      index = "0",
      description =
          "source catalog type. "
              + "Can be one of these [CUSTOM, DYNAMODB, ECS, GLUE, HADOOP, HIVE, JDBC, NESSIE, REST]")
  private CatalogType sourceCatalogType;

  @CommandLine.Parameters(index = "1", split = ",", description = "source catalog properties")
  private Map<String, String> sourceCatalogProperties;

  @CommandLine.Parameters(
      index = "2",
      description =
          "target catalog type. "
              + "Can be one of these [CUSTOM, DYNAMODB, ECS, GLUE, HADOOP, HIVE, JDBC, NESSIE, REST]")
  private CatalogType targetCatalogType;

  @CommandLine.Parameters(index = "3", split = ",", description = "target catalog properties")
  private Map<String, String> targetCatalogProperties;

  public static void main(String... args) {
    CommandLine commandLine = new CommandLine(new CatalogMigrationCLI());
    commandLine.setUsageHelpWidth(150);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() {
    if (identifiersFromFile != null && !identifiers.isEmpty()) {
      throw new IllegalArgumentException(
          "Both `--identifiers` and `--identifiers-from-file` options are configured. Please use only one of them.");
    } else if (identifiersFromFile != null) {
      if (!Files.exists(Paths.get(identifiersFromFile))) {
        throw new IllegalArgumentException(
            "File specified in `--identifiers-from-file` option does not exist.");
      }
    }

    PrintWriter printWriter = commandSpec.commandLine().getOut();
    Configuration sourceCatalogConf = new Configuration();
    if (sourceHadoopConf != null && !sourceHadoopConf.isEmpty()) {
      sourceHadoopConf.forEach(sourceCatalogConf::set);
    }
    Catalog sourceCatalog =
        CatalogUtil.loadCatalog(
            Objects.requireNonNull(catalogImpl(sourceCatalogType, sourceCustomCatalogImpl)),
            "sourceCatalog",
            sourceCatalogProperties,
            sourceCatalogConf);
    printWriter.printf("\nConfigured source catalog: %s\n", sourceCatalogType.name());

    Configuration targetCatalogConf = new Configuration();
    if (targetHadoopConf != null && !targetHadoopConf.isEmpty()) {
      targetHadoopConf.forEach(targetCatalogConf::set);
    }
    Catalog targetCatalog =
        CatalogUtil.loadCatalog(
            Objects.requireNonNull(catalogImpl(targetCatalogType, targetCustomCatalogImpl)),
            "targetCatalog",
            targetCatalogProperties,
            targetCatalogConf);
    printWriter.printf("\nConfigured target catalog: %s\n", targetCatalogType.name());

    List<TableIdentifier> tableIdentifiers = null;
    if (identifiersFromFile != null) {
      try {
        printWriter.printf("\nCollecting identifiers from the file %s...\n", identifiersFromFile);
        tableIdentifiers =
            Files.readAllLines(Paths.get(identifiersFromFile)).stream()
                .map(TableIdentifier::parse)
                .collect(Collectors.toList());
      } catch (IOException e) {
        throw new RuntimeException("Failed to read the file:", e);
      }
    } else if (!identifiers.isEmpty()) {
      tableIdentifiers =
          identifiers.stream().map(TableIdentifier::parse).collect(Collectors.toList());
    }

    ImmutablePair<Collection<TableIdentifier>, Collection<TableIdentifier>> result;
    if (deleteSourceCatalogTables) {
      result =
          CatalogMigrateUtil.migrateTables(
              tableIdentifiers, sourceCatalog, targetCatalog, maxThreadPoolSize, printWriter);
      if (sourceCatalogType == CatalogType.HADOOP) {
        printWriter.println(
            "\n[WARNING]: Source catalog type is HADOOP and it doesn't support dropping tables just from "
                + "catalog. \nAvoid operating the migrated tables from the source catalog after migration. "
                + "Use the tables from target catalog.");
      }
    } else {
      result =
          CatalogMigrateUtil.registerTables(
              tableIdentifiers, sourceCatalog, targetCatalog, maxThreadPoolSize, printWriter);
    }

    printWriter.println("\nSummary: ");
    if (!result.left.isEmpty()) {
      printWriter.printf(
          "- Successfully %s %d tables from %s catalog to %s catalog. \n",
          deleteSourceCatalogTables ? "migrated" : "registered",
          result.left.size(),
          sourceCatalogType.name(),
          targetCatalogType.name());
    }
    if (!result.right.isEmpty()) {
      List<String> failedIdentifiers =
          result.right.stream().map(TableIdentifier::toString).collect(Collectors.toList());
      try {
        Files.write(Paths.get("failed_identifiers.txt"), failedIdentifiers);
      } catch (IOException e) {
        throw new RuntimeException("Failed to write the file:", e);
      }
      printWriter.printf(
          "- Failed to %s %d tables from %s catalog to %s catalog. "
              + "Please check the `catalog_migration.log` file for the failure reason. "
              + "\n Failed Identifiers are written to `failed_identifiers.txt`. "
              + "Retry with that file using `--identifiers-from-file` option "
              + "if the failure is because of network/connection timeouts.\n",
          deleteSourceCatalogTables ? "migrate" : "register",
          result.right.size(),
          sourceCatalogType.name(),
          targetCatalogType.name());
    }
    printWriter.println("\nDetails: ");
    if (!result.left.isEmpty()) {
      printWriter.printf(
          "- Successfully %s these tables: \n",
          deleteSourceCatalogTables ? "migrated" : "registered");
      printWriter.println(result.left);
    }
    if (!result.right.isEmpty()) {
      printWriter.printf(
          "- Failed to %s these tables: \n", deleteSourceCatalogTables ? "migrate" : "register");
      printWriter.println(result.right);
    }

    return 0;
  }

  private String catalogImpl(CatalogType type, String customCatalogImpl) {
    switch (type) {
      case CUSTOM:
        if (customCatalogImpl == null || customCatalogImpl.isEmpty()) {
          throw new IllegalArgumentException(
              "Need to specify the fully qualified class name of the custom catalog " + "impl");
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
      default:
        throw new IllegalArgumentException("Unsupported type: " + type.name());
    }
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
