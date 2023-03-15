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

import static org.projectnessie.tools.catalog.migration.cli.PromptUtil.ANSI_YELLOW;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.projectnessie.tools.catalog.migration.api.CatalogMigrator;
import org.projectnessie.tools.catalog.migration.api.ImmutableCatalogMigrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
    name = "migrate",
    mixinStandardHelpOptions = true,
    versionProvider = CLIVersionProvider.class,
    // As both source and target catalog has similar configurations,
    // documentation is easy to read if the target and source property is one after another instead
    // of sorted order.
    sortOptions = false,
    description =
        "Bulk migrate the iceberg tables from source catalog to target catalog without data copy."
            + " Table entries from source catalog will be deleted after the successful migration to the target "
            + "catalog.")
public class MigrateCommand extends BaseRegisterCommand {

  private final Logger consoleLog = LoggerFactory.getLogger("console-log");

  @Override
  protected CatalogMigrator catalogMigrator(Catalog sourceCatalog, Catalog targetCatalog) {
    return ImmutableCatalogMigrator.builder()
        .sourceCatalog(sourceCatalog)
        .targetCatalog(targetCatalog)
        .deleteEntriesFromSourceCatalog(true)
        .build();
  }

  @Override
  protected boolean canProceed(Catalog sourceCatalog) {
    if (sourceCatalog instanceof HadoopCatalog) {
      consoleLog.warn(
          "{}Source catalog type is HADOOP and it doesn't support dropping tables just from "
              + "catalog. {}Avoid operating the migrated tables from the source catalog after migration. "
              + "Use the tables from target catalog.",
          ANSI_YELLOW,
          System.lineSeparator());
    }
    return PromptUtil.proceedForMigration();
  }

  @Override
  protected String operation() {
    return "migration";
  }

  @Override
  protected String operated() {
    return "migrated";
  }

  @Override
  protected String operate() {
    return "migrate";
  }
}
