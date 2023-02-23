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
package org.projectnessie.tools.catlog.migration.cli;

import static org.projectnessie.tools.catalog.migration.cli.PromptUtil.WARNING_FOR_MIGRATION;
import static org.projectnessie.tools.catalog.migration.cli.PromptUtil.WARNING_FOR_REGISTRATION;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.NoSuchElementException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.projectnessie.tools.catalog.migration.cli.PromptUtil;

public class PromptUtilTest {

  @ParameterizedTest
  @CsvSource({"yes, false", "yes, true", "no, false", "no, true", "dummy, false", "dummy, true"})
  public void testPrompts(String input, boolean deleteSourceTables) throws Exception {
    String warning = deleteSourceTables ? WARNING_FOR_MIGRATION : WARNING_FOR_REGISTRATION;
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(input.getBytes());
    try {
      switch (input) {
        case "yes":
          Assertions.assertThat(callPrompt(deleteSourceTables, inputStream, printWriter)).isTrue();
          Assertions.assertThat(stringWriter.toString()).contains(warning);
          Assertions.assertThat(stringWriter.toString())
              .contains(
                  "Have you read the above warnings and are you sure you want to continue? (yes/no):");
          Assertions.assertThat(stringWriter.toString()).contains("Continuing...");
          break;
        case "no":
          Assertions.assertThat(callPrompt(deleteSourceTables, inputStream, printWriter)).isFalse();
          Assertions.assertThat(stringWriter.toString()).contains(warning);
          Assertions.assertThat(stringWriter.toString())
              .contains(
                  "Have you read the above warnings and are you sure you want to continue? (yes/no):");
          Assertions.assertThat(stringWriter.toString()).contains("Aborting...");
          break;
        case "dummy":
          Assertions.assertThatThrownBy(
                  () -> callPrompt(deleteSourceTables, inputStream, printWriter))
              .isInstanceOf(NoSuchElementException.class)
              .hasMessageContaining("No line found");
          Assertions.assertThat(stringWriter.toString())
              .contains("Invalid input. Please enter 'yes' or 'no'.");
          break;
        default:
      }
    } finally {
      inputStream.close();
      stringWriter.close();
      printWriter.close();
    }
  }

  private boolean callPrompt(
      boolean deleteSourceTables, InputStream inputStream, PrintWriter printWriter) {
    return deleteSourceTables
        ? PromptUtil.proceedForMigration(inputStream, printWriter)
        : PromptUtil.proceedForRegistration(inputStream, printWriter);
  }
}
