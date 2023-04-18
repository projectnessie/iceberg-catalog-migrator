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

import org.apache.iceberg.catalog.Catalog;
import org.projectnessie.tools.catalog.migration.api.CatalogMigrator;
import org.projectnessie.tools.catalog.migration.api.ImmutableCatalogMigrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
        "Bulk register the iceberg tables from source catalog to target catalog without data copy.")
public class RegisterCommand extends BaseRegisterCommand {

  private static final String newLine = System.lineSeparator();
  private static final Logger consoleLog = LoggerFactory.getLogger("console-log");

  @Override
  protected CatalogMigrator catalogMigrator(
      Catalog sourceCatalog, Catalog targetCatalog, boolean enableStackTrace) {
    return ImmutableCatalogMigrator.builder()
        .sourceCatalog(sourceCatalog)
        .targetCatalog(targetCatalog)
        .deleteEntriesFromSourceCatalog(false)
        .enableStacktrace(enableStackTrace)
        .build();
  }

  @Override
  protected boolean canProceed(Catalog sourceCatalog) {
    consoleLog.warn(
        "{}"
            + "\ta) Executing catalog migration when the source catalog has some in-progress commits "
            + "{}\tcan lead to a data loss as the in-progress commits will not be considered for migration. "
            + "{}\tSo, while using this tool please make sure there are no in-progress commits for the source "
            + "catalog.{}"
            + "{}"
            + "\tb) After the registration, successfully registered tables will be present in both source and target "
            + "catalog. "
            + "{}\tHaving the same metadata.json registered in more than one catalog can lead to missing updates, "
            + "loss of data, and table corruption. "
            + "{}\tUse `migrate` command to automatically delete the table from source catalog after "
            + "migration.",
        newLine,
        newLine,
        newLine,
        newLine,
        newLine,
        newLine,
        newLine);
    return proceed();
  }

  @Override
  protected String operation() {
    return "registration";
  }

  @Override
  protected String operated() {
    return "registered";
  }

  @Override
  protected String operate() {
    return "register";
  }
}
