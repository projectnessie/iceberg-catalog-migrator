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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
    name = "iceberg-catalog-migrator",
    mixinStandardHelpOptions = true,
    versionProvider = CLIVersionProvider.class,
    subcommands = {MigrateCommand.class, RegisterCommand.class})
public class CatalogMigrationCLI {

  public CatalogMigrationCLI() {}

  private static final Logger consoleLog = LoggerFactory.getLogger("console-log");

  public static void main(String... args) {
    CommandLine commandLine =
        new CommandLine(new CatalogMigrationCLI())
            .setExecutionExceptionHandler(
                (ex, cmd, parseResult) -> {
                  if (enableStacktrace(args)) {
                    cmd.getErr().println(cmd.getColorScheme().richStackTraceString(ex));
                  } else {
                    consoleLog.error(
                        "Error during CLI execution: {}. Please check `catalog_migration.log` file for more info.",
                        ex.getMessage());
                  }
                  return 1;
                });
    commandLine.setUsageHelpWidth(150);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  private static boolean enableStacktrace(String... args) {
    for (String arg : args) {
      if (arg.equalsIgnoreCase("--stacktrace")) {
        return true;
      }
    }
    return false;
  }
}
