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

import java.io.Console;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PromptUtil {

  private PromptUtil() {}

  private static final Logger consoleLog = LoggerFactory.getLogger("console-log");

  private static final String WARNING_FOR_REGISTRATION =
      String.format(
          "%n"
              + "\ta) Executing catalog migration when the source catalog has some in-progress commits "
              + "%n\tcan lead to a data loss as the in-progress commit will not be considered for migration. "
              + "%n\tSo, while using this tool please make sure there are no in-progress commits for the source "
              + "catalog%n"
              + "%n"
              + "\tb) After the registration, successfully registered tables will be present in both source and target "
              + "catalog. "
              + "%n\tHaving the same metadata.json registered in more than one catalog can lead to missing updates, "
              + "loss of data, and table corruption. "
              + "%n\tUse `migrate` command to automatically delete the table from source catalog after "
              + "migration.");

  private static final String WARNING_FOR_MIGRATION =
      String.format(
          "%n"
              + "\ta) Executing catalog migration when the source catalog has some in-progress commits "
              + "%n\tcan lead to a data loss as the in-progress commit will not be considered for migration. "
              + "%n\tSo, while using this tool please make sure there are no in-progress commits for the source "
              + "catalog%n"
              + "%n"
              + "\tb) After the migration, successfully migrated tables will be deleted from the source catalog "
              + "%n\tand can only be accessed from the target catalog.");

  static boolean proceedForRegistration() {
    return proceed(WARNING_FOR_REGISTRATION);
  }

  static boolean proceedForMigration() {
    return proceed(WARNING_FOR_MIGRATION);
  }

  private static boolean proceed(String warning) {
    consoleLog.warn(warning);

    Console console = System.console();
    while (true) {
      consoleLog.info(
          "Have you read the above warnings and are you sure you want to continue? (yes/no):");
      String input = console.readLine();

      if (input.equalsIgnoreCase("yes")) {
        consoleLog.info("Continuing...");
        return true;
      } else if (input.equalsIgnoreCase("no")) {
        consoleLog.info("Aborting...");
        return false;
      } else {
        consoleLog.info("Invalid input. Please enter 'yes' or 'no'.");
      }
    }
  }
}
