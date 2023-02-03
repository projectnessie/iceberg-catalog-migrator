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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
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

@CommandLine.Command(
    name = "register",
    mixinStandardHelpOptions = true,
    versionProvider = CLIVersionProvider.class,
    // As both source and target catalog has similar configurations,
    // documentation is easy to read if the target and source property is one after another instead
    // of sorted order.
    sortOptions = false,
    description =
        "\nBulk register the iceberg tables from source catalog to target catalog without data copy.\n")
public class CatalogMigrationCLI implements Callable<Integer> {
  @CommandLine.Spec CommandLine.Model.CommandSpec commandSpec;

  @CommandLine.Option(
      names = "--source-catalog-type",
      required = true,
      description =
          "source catalog type. "
              + "Can be one of these [CUSTOM, DYNAMODB, ECS, GLUE, HADOOP, HIVE, JDBC, NESSIE, REST]")
  private CatalogType sourceCatalogType;

  @CommandLine.Option(
      names = "--source-catalog-properties",
      required = true,
      split = ",",
      description = "source catalog properties (like uri, warehouse, etc)")
  private Map<String, String> sourceCatalogProperties;

  @CommandLine.Option(
      names = "--source-catalog-hadoop-conf",
      split = ",",
      description =
          "optional source catalog Hadoop configurations (like fs.s3a.secret.key, fs.s3a.access.key) required when "
              + "using an Iceberg FileIO.")
  Map<String, String> sourceHadoopConf = new HashMap<>();

  @CommandLine.Option(
      names = {"--source-custom-catalog-impl"},
      description =
          "optional fully qualified class name of the custom catalog implementation of the source catalog. Required "
              + "when the catalog type is CUSTOM.")
  String sourceCustomCatalogImpl;

  @CommandLine.Option(
      names = "--target-catalog-type",
      required = true,
      description =
          "target catalog type. "
              + "Can be one of these [CUSTOM, DYNAMODB, ECS, GLUE, HADOOP, HIVE, JDBC, NESSIE, REST]")
  private CatalogType targetCatalogType;

  @CommandLine.Option(
      names = "--target-catalog-properties",
      required = true,
      split = ",",
      description = "target catalog properties (like uri, warehouse, etc)")
  private Map<String, String> targetCatalogProperties;

  @CommandLine.Option(
      names = "--target-catalog-hadoop-conf",
      split = ",",
      description =
          "optional target catalog Hadoop configurations (like fs.s3a.secret.key, fs.s3a.access.key) required when "
              + "using an Iceberg FileIO.")
  Map<String, String> targetHadoopConf = new HashMap<>();

  @CommandLine.Option(
      names = {"--target-custom-catalog-impl"},
      description =
          "optional fully qualified class name of the custom catalog implementation of the target catalog. Required "
              + "when the catalog type is CUSTOM.")
  String targetCustomCatalogImpl;

  @CommandLine.Option(
      names = {"--identifiers"},
      split = ",",
      description =
          "optional selective list of identifiers to register. If not specified, all the tables will be registered. "
              + "Use this when there are few identifiers that need to be registered. For a large number of identifiers, "
              + "use the `--identifiers-from-file` or `--identifiers-regex` option.")
  List<String> identifiers = new ArrayList<>();

  @CommandLine.Option(
      names = {"--identifiers-from-file"},
      description =
          "optional text file path that contains a list of table identifiers (one per line) to register. Should not be "
              + "used with `--identifiers` or `--identifiers-regex` option.")
  String identifiersFromFile;

  @CommandLine.Option(
      names = {"--identifiers-regex"},
      description =
          "optional regular expression pattern used to register only the tables whose identifiers match this pattern. "
              + "Should not be used with `--identifiers` or '--identifiers-from-file' option.")
  String identifiersRegEx;

  @CommandLine.Option(
      names = {"--dry-run"},
      description =
          "optional configuration to simulate the registration without actually registering. Can learn about a list "
              + "of the tables that will be registered by running this.")
  private boolean isDryRun;

  @CommandLine.Option(
      names = {"--delete-source-tables"},
      description =
          "optional configuration to delete the table entry from source catalog after successfully registering it "
              + "to target catalog.")
  private boolean deleteSourceCatalogTables;

  static final String FAILED_IDENTIFIERS_FILE = "failed_identifiers.txt";
  static final String FAILED_TO_DELETE_AT_SOURCE_FILE = "failed_to_delete_at_source.txt";
  static final String DRY_RUN_FILE = "dry_run_identifiers.txt";

  public static void main(String... args) {
    CommandLine commandLine = new CommandLine(new CatalogMigrationCLI());
    commandLine.setUsageHelpWidth(150);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() {
    validateIdentifierOptions();

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
    printWriter.println(String.format("\nConfigured source catalog: %s", sourceCatalogType.name()));

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
    printWriter.println(String.format("\nConfigured target catalog: %s", targetCatalogType.name()));

    List<TableIdentifier> tableIdentifiers = null;
    if (identifiersFromFile != null) {
      try {
        printWriter.println(
            String.format("Collecting identifiers from the file %s...", identifiersFromFile));
        printWriter.println();
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

    CatalogMigrateUtil.CatalogMigrationResult result;
    if (deleteSourceCatalogTables) {
      if (!isDryRun && !proceedForMigration(printWriter)) {
        return 0;
      }

      result =
          CatalogMigrateUtil.migrateTables(
              tableIdentifiers,
              sourceCatalog,
              targetCatalog,
              identifiersRegEx,
              isDryRun,
              printWriter);
      if (sourceCatalogType == CatalogType.HADOOP) {
        printWriter.println(
            "[WARNING]: Source catalog type is HADOOP and it doesn't support dropping tables just from "
                + "catalog. \nAvoid operating the migrated tables from the source catalog after migration. "
                + "Use the tables from target catalog.");
      }
    } else {
      if (!isDryRun && !proceedForRegistration(printWriter)) {
        return 0;
      }

      result =
          CatalogMigrateUtil.registerTables(
              tableIdentifiers,
              sourceCatalog,
              targetCatalog,
              identifiersRegEx,
              isDryRun,
              printWriter);
    }

    if (isDryRun) {
      printWriter.println("Dry run is completed.");
      printDryRunResults(printWriter, result);
      return 0;
    }

    printSummary(printWriter, result);

    printDetails(printWriter, result);

    return 0;
  }

  private void printSummary(
      PrintWriter printWriter, CatalogMigrateUtil.CatalogMigrationResult result) {
    printWriter.println("\nSummary: ");
    if (!result.registeredTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- Successfully %s %d tables from %s catalog to %s catalog.",
              deleteSourceCatalogTables ? "migrated" : "registered",
              result.registeredTableIdentifiers().size(),
              sourceCatalogType.name(),
              targetCatalogType.name()));
    }
    if (!result.failedToRegisterTableIdentifiers().isEmpty()) {
      writeToFile(FAILED_IDENTIFIERS_FILE, result.failedToRegisterTableIdentifiers());
      printWriter.println(
          String.format(
              "- Failed to %s %d tables from %s catalog to %s catalog. "
                  + "Please check the `catalog_migration.log` file for the failure reason. "
                  + "\nFailed identifiers are written into `%s`. "
                  + "Retry with that file using `--identifiers-from-file` option "
                  + "if the failure is because of network/connection timeouts.",
              deleteSourceCatalogTables ? "migrate" : "register",
              result.failedToRegisterTableIdentifiers().size(),
              sourceCatalogType.name(),
              targetCatalogType.name(),
              FAILED_IDENTIFIERS_FILE));
    }
    if (!result.failedToDeleteTableIdentifiers().isEmpty()) {
      writeToFile(FAILED_TO_DELETE_AT_SOURCE_FILE, result.failedToDeleteTableIdentifiers());
      printWriter.println(
          String.format(
              "- Failed to delete %d tables from %s catalog. "
                  + "Please check the `catalog_migration.log` file for the reason. "
                  + "\nFailed to delete identifiers are written into `%s`. ",
              result.failedToDeleteTableIdentifiers().size(),
              sourceCatalogType.name(),
              FAILED_TO_DELETE_AT_SOURCE_FILE));
    }
  }

  private void printDetails(
      PrintWriter printWriter, CatalogMigrateUtil.CatalogMigrationResult result) {
    printWriter.println("\nDetails: ");
    if (!result.registeredTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- Successfully %s these tables:",
              deleteSourceCatalogTables ? "migrated" : "registered"));
      printWriter.println(result.registeredTableIdentifiers());
    }

    if (!result.failedToRegisterTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- Failed to %s these tables:", deleteSourceCatalogTables ? "migrate" : "register"));
      printWriter.println(result.failedToRegisterTableIdentifiers());
    }

    if (!result.failedToDeleteTableIdentifiers().isEmpty()) {
      printWriter.println("- [WARNING] Failed to delete these tables from source catalog:");
      printWriter.println(result.failedToDeleteTableIdentifiers());
    }
  }

  private void printDryRunResults(
      PrintWriter printWriter, CatalogMigrateUtil.CatalogMigrationResult result) {
    printWriter.println("\nSummary: ");
    if (result.registeredTableIdentifiers().isEmpty()) {
      printWriter.println(
          String.format(
              "- No tables are identified for %s. Please check logs for more info.",
              deleteSourceCatalogTables ? "migration" : "registration"));
      return;
    }
    writeToFile(DRY_RUN_FILE, result.registeredTableIdentifiers());
    printWriter.println(
        String.format(
            "- Identified %d tables for %s by dry-run. These identifiers are also written into %s. "
                + "You can use this file with `--identifiers-from-file` option.",
            result.registeredTableIdentifiers().size(),
            deleteSourceCatalogTables ? "migration" : "registration",
            DRY_RUN_FILE));

    printWriter.println("\nDetails: ");
    printWriter.println(
        String.format(
            "- Identified these tables for %s by dry-run:",
            deleteSourceCatalogTables ? "migration" : "registration"));
    printWriter.println(result.registeredTableIdentifiers());
  }

  private void writeToFile(String filePath, List<TableIdentifier> identifiers) {
    List<String> identifiersString =
        identifiers.stream().map(TableIdentifier::toString).collect(Collectors.toList());
    try {
      Files.write(Paths.get(filePath), identifiersString);
    } catch (IOException e) {
      throw new RuntimeException("Failed to write the file:" + filePath, e);
    }
  }

  private void validateIdentifierOptions() {
    if (identifiersFromFile != null && !identifiers.isEmpty() && identifiersRegEx != null) {
      throw new IllegalArgumentException(
          "All the three identifier options (`--identifiers`, `--identifiers-from-file`, "
              + "`--identifiers-regex`) are configured. Please use only one of them.");
    } else if (identifiersFromFile != null) {
      if (!identifiers.isEmpty()) {
        throw new IllegalArgumentException(
            "Both `--identifiers` and `--identifiers-from-file` options are configured. Please use only one of them.");
      } else if (identifiersRegEx != null) {
        throw new IllegalArgumentException(
            "Both `--identifiers-regex` and `--identifiers-from-file` options are configured. Please use only one of them.");
      } else {
        if (!Files.exists(Paths.get(identifiersFromFile))) {
          throw new IllegalArgumentException(
              "File specified in `--identifiers-from-file` option does not exist.");
        }
      }
    } else if (!identifiers.isEmpty()) {
      if (identifiersRegEx != null) {
        throw new IllegalArgumentException(
            "Both `--identifiers-regex` and `--identifiers` options are configured. Please use only one of them.");
      }
    }
  }

  private static boolean proceedForRegistration(PrintWriter printWriter) {
    String warning =
        "\n[WARNING]\n"
            + "\ta) Executing catalog migration when the source catalog has some in-progress commits "
            + "\n\tcan lead to a data loss as the in-progress commit will not be considered for migration. "
            + "\n\tSo, while using this tool please make sure there are no in-progress commits for the source "
            + "catalog\n"
            + "\n"
            + "\tb) After the registration, successfully registered tables will be present in both source and target "
            + "catalog. "
            + "\n\tHaving the same metadata.json registered in more than one catalog can lead to missing updates, "
            + "loss"
            + " of data, and table corruption. "
            + "\n\tUse `--delete-source-tables` option to automatically delete the table from source catalog after "
            + "migration.";
    return proceed(warning, printWriter);
  }

  private static boolean proceedForMigration(PrintWriter printWriter) {
    String warning =
        "\n[WARNING]\n"
            + "\ta) Executing catalog migration when the source catalog has some in-progress commits "
            + "\n\tcan lead to a data loss as the in-progress commit will not be considered for migration. "
            + "\n\tSo, while using this tool please make sure there are no in-progress commits for the source "
            + "catalog\n"
            + "\n"
            + "\tb) After the migration, successfully migrated tables will be deleted from the source catalog "
            + "\n\tand can only be accessed from the target catalog.";
    return proceed(warning, printWriter);
  }

  private static boolean proceed(String warning, PrintWriter printWriter) {
    try (Scanner scanner = new Scanner(System.in)) {
      printWriter.println(warning);

      while (true) {
        printWriter.println(
            "Have you read the above warnings and are you sure you want to continue? (yes/no):");
        String input = scanner.nextLine();

        if (input.equalsIgnoreCase("yes")) {
          printWriter.println("Continuing...");
          return true;
        } else if (input.equalsIgnoreCase("no")) {
          printWriter.println("Aborting...");
          return false;
        } else {
          printWriter.println("Invalid input. Please enter 'yes' or 'no'.");
        }
      }
    }
  }

  private static String catalogImpl(CatalogType type, String customCatalogImpl) {
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
