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

import java.io.PrintWriter;
import java.util.Scanner;

public final class PromptUtil {

  private PromptUtil() {}

  static boolean proceedForRegistration(PrintWriter printWriter) {
    String warning =
        String.format(
            "%n[WARNING]%n"
                + "\ta) Executing catalog migration when the source catalog has some in-progress commits "
                + "%n\tcan lead to a data loss as the in-progress commit will not be considered for migration. "
                + "%n\tSo, while using this tool please make sure there are no in-progress commits for the source "
                + "catalog%n"
                + "%n"
                + "\tb) After the registration, successfully registered tables will be present in both source and target "
                + "catalog. "
                + "%n\tHaving the same metadata.json registered in more than one catalog can lead to missing updates, "
                + "loss of data, and table corruption. "
                + "%n\tUse `--delete-source-tables` option to automatically delete the table from source catalog after "
                + "migration.");
    return proceed(warning, printWriter);
  }

  static boolean proceedForMigration(PrintWriter printWriter) {
    String warning =
        String.format(
            "%n[WARNING]%n"
                + "\ta) Executing catalog migration when the source catalog has some in-progress commits "
                + "%n\tcan lead to a data loss as the in-progress commit will not be considered for migration. "
                + "%n\tSo, while using this tool please make sure there are no in-progress commits for the source "
                + "catalog%n"
                + "%n"
                + "\tb) After the migration, successfully migrated tables will be deleted from the source catalog "
                + "%n\tand can only be accessed from the target catalog.");
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
}
