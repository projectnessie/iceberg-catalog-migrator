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

import picocli.CommandLine;

@CommandLine.Command(
    name = "iceberg-catalog-migrator",
    mixinStandardHelpOptions = true,
    versionProvider = CLIVersionProvider.class,
    subcommands = {MigrateCommand.class, RegisterCommand.class})
public class CatalogMigrationCLI {

  public CatalogMigrationCLI() {}

  public static void main(String... args) {
    CommandLine commandLine =
        new CommandLine(new CatalogMigrationCLI()).setColorScheme(createColorScheme());
    commandLine.setUsageHelpWidth(150);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  private static CommandLine.Help.ColorScheme createColorScheme() {
    return new CommandLine.Help.ColorScheme.Builder()
        .commands(
            CommandLine.Help.Ansi.Style.bold,
            CommandLine.Help.Ansi.Style.underline) // combine multiple styles
        .options(CommandLine.Help.Ansi.Style.fg_yellow) // yellow foreground color
        .parameters(CommandLine.Help.Ansi.Style.fg_yellow)
        .optionParams(CommandLine.Help.Ansi.Style.italic)
        .errors(CommandLine.Help.Ansi.Style.fg_red, CommandLine.Help.Ansi.Style.bold)
        .stackTraces(CommandLine.Help.Ansi.Style.italic)
        .build();
  }
}
