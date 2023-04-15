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

import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CLIOptionsTest {

  protected static @TempDir Path logDir;

  @BeforeAll
  protected static void initLogDir() {
    System.setProperty("catalog.migration.log.dir", logDir.toAbsolutePath().toString());
  }

  private static Stream<Arguments> optionErrors() {
    return Stream.of(
        // no arguments
        arguments(
            Lists.newArrayList(),
            "Error: Missing required argument(s): (--target-catalog-type=<type> --target-catalog-properties=<String=String>[,<String=String>...] [--target-catalog-properties=<String=String>[,<String=String>...]]... [--target-catalog-hadoop-conf=<String=String>[,<String=String>...]]... [--target-custom-catalog-impl=<customCatalogImpl>])"),
        // missing required arguments
        arguments(Lists.newArrayList(""), "Unmatched argument at index 1: ''"),
        // missing required arguments
        arguments(
            Lists.newArrayList(
                "--source-catalog-properties", "properties1=ab", "--target-catalog-type", "NESSIE"),
            "Error: Missing required argument(s): --source-catalog-type=<type>"),
        // missing required arguments
        arguments(
            Lists.newArrayList("--source-catalog-type", "GLUE"),
            "Error: Missing required argument(s): --source-catalog-properties=<String=String>"),
        // missing required arguments
        arguments(
            Lists.newArrayList(
                "--source-catalog-type",
                "HIVE",
                "--source-catalog-properties",
                "properties1=ab",
                "--target-catalog-type",
                "NESSIE"),
            "Error: Missing required argument(s): --target-catalog-properties=<String=String>"),
        // missing required arguments
        arguments(
            Lists.newArrayList(
                "--source-catalog-type",
                "HIVE",
                "--source-catalog-properties",
                "properties1=ab",
                "--target-catalog-properties",
                "properties2=cd"),
            "Error: Missing required argument(s): --target-catalog-type=<type>"),
        arguments(
            Lists.newArrayList(
                "--source-catalog-type",
                "HADOOP",
                "--source-catalog-properties",
                "k1=v1,k2=v2",
                "--target-catalog-type",
                "HIVE",
                "--target-catalog-properties",
                "k3=v3, k4=v4",
                "--identifiers",
                "foo.tbl",
                "--identifiers-from-file",
                "file.txt",
                "--identifiers-regex",
                "^foo\\."),
            "Error: --identifiers=<identifiers>, --identifiers-from-file=<identifiersFromFile>, --identifiers-regex=<identifiersRegEx> are mutually exclusive (specify only one)"),
        arguments(
            Lists.newArrayList(
                "--source-catalog-type",
                "HADOOP",
                "--source-catalog-properties",
                "k1=v1,k2=v2",
                "--target-catalog-type",
                "HIVE",
                "--target-catalog-properties",
                "k3=v3, k4=v4",
                "--identifiers",
                "foo.tbl",
                "--identifiers-from-file",
                "file.txt"),
            "Error: --identifiers=<identifiers>, --identifiers-from-file=<identifiersFromFile> are mutually exclusive (specify only one)"),
        arguments(
            Lists.newArrayList(
                "--source-catalog-type",
                "HADOOP",
                "--source-catalog-properties",
                "k1=v1,k2=v2",
                "--target-catalog-type",
                "HIVE",
                "--target-catalog-properties",
                "k3=v3, k4=v4",
                "--identifiers-regex",
                "^foo\\.",
                "--identifiers-from-file",
                "file.txt"),
            "Error: --identifiers-from-file=<identifiersFromFile>, --identifiers-regex=<identifiersRegEx> are mutually exclusive (specify only one)"),
        arguments(
            Lists.newArrayList(
                "--source-catalog-type",
                "HADOOP",
                "--source-catalog-properties",
                "k1=v1,k2=v2",
                "--target-catalog-type",
                "HIVE",
                "--target-catalog-properties",
                "k3=v3, k4=v4",
                "--identifiers",
                "foo.tbl",
                "--identifiers-regex",
                "^foo\\."),
            "Error: --identifiers=<identifiers>, --identifiers-regex=<identifiersRegEx> are mutually exclusive "
                + "(specify only one)"));
  }

  @ParameterizedTest
  @MethodSource("optionErrors")
  public void testOptionErrorsForRegister(List<String> args, String expectedMessage)
      throws Exception {
    executeAndValidateResults("register", args, expectedMessage, 2);
  }

  @ParameterizedTest
  @MethodSource("optionErrors")
  public void testOptionErrorsForMigrate(List<String> args, String expectedMessage)
      throws Exception {
    executeAndValidateResults("migrate", args, expectedMessage, 2);
  }

  private static Stream<Arguments> invalidArgs() {
    return Stream.of(
        arguments(
            Lists.newArrayList(
                "--source-catalog-type",
                "HIVE",
                "--source-catalog-properties",
                "k1=v1,k2=v2",
                "--target-catalog-type",
                "HADOOP",
                "--target-catalog-properties",
                "k3=v3, k4=v4"),
            "Error during CLI execution: Cannot initialize HadoopCatalog "
                + "because warehousePath must not be null or empty"),
        arguments(
            Lists.newArrayList(
                "--source-catalog-type",
                "HIVE",
                "--source-catalog-properties",
                "k1=v1,k2=v2",
                "--target-catalog-type",
                "HADOOP",
                "--target-catalog-properties",
                "k3=v3, k4=v4",
                "--identifiers-from-file",
                "file.txt"),
            "Error during CLI execution: File specified in `--identifiers-from-file` option does not exist"),
        arguments(
            Lists.newArrayList(
                "--source-catalog-type",
                "HIVE",
                "--source-catalog-properties",
                "k1=v1,k2=v2",
                "--target-catalog-type",
                "HADOOP",
                "--target-catalog-properties",
                "k3=v3, k4=v4",
                "--output-dir",
                "/path/to/file"),
            "Error during CLI execution: Failed to create the output directory from the path specified in `--output-dir`"),
        arguments(
            Lists.newArrayList(
                "--source-catalog-type",
                "HIVE",
                "--source-catalog-properties",
                "k1=v1,k2=v2",
                "--target-catalog-type",
                "HADOOP",
                "--target-catalog-properties",
                "k3=v3, k4=v4",
                "--output-dir",
                readOnlyDirLocation()),
            "Error during CLI execution: Path specified in `--output-dir` is not writable"),
        // test with stacktrace
        arguments(
            Lists.newArrayList(
                "--source-catalog-type",
                "HIVE",
                "--source-catalog-properties",
                "k1=v1,k2=v2",
                "--target-catalog-type",
                "HADOOP",
                "--target-catalog-properties",
                "k3=v3, k4=v4",
                "--output-dir",
                readOnlyDirLocation(),
                "--stacktrace"),
            "java.lang.IllegalArgumentException: Path specified in `--output-dir` is not writable"));
  }

  @ParameterizedTest
  @MethodSource("invalidArgs")
  public void testInvalidArgsForRegister(List<String> args, String expectedMessage)
      throws Exception {
    executeAndValidateResults("register", args, expectedMessage, 1);
  }

  @ParameterizedTest
  @MethodSource("invalidArgs")
  public void testInvalidArgsForMigrate(List<String> args, String expectedMessage)
      throws Exception {
    executeAndValidateResults("migrate", args, expectedMessage, 1);
  }

  @Test
  public void version() throws Exception {
    RunCLI run = RunCLI.runWithPrintWriter("--version");
    Assertions.assertThat(run.getExitCode()).isEqualTo(0);
    Assertions.assertThat(run.getOut()).startsWith(System.getProperty("expectedCLIVersion"));
  }

  private static void executeAndValidateResults(
      String command, List<String> args, String expectedMessage, int expectedErrorCode)
      throws Exception {
    args.add(0, command);
    RunCLI run = RunCLI.run(args);

    Assertions.assertThat(run.getExitCode()).isEqualTo(expectedErrorCode);
    Assertions.assertThat(run.getErr()).contains(expectedMessage);
  }

  private static String readOnlyDirLocation() {
    Path readOnly = logDir.resolve(UUID.randomUUID().toString());
    try {
      Files.createDirectory(readOnly);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Assertions.assertThat(readOnly.toFile().setWritable(false)).isTrue();

    return readOnly.toAbsolutePath().toString();
  }
}
