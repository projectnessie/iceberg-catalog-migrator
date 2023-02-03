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

import static java.util.Collections.singletonList;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CLIErrorsTest {

  static Stream<Arguments> optionErrors() {
    return Stream.of(
        // no arguments
        arguments(
            Collections.emptyList(),
            "Missing required options: '--source-catalog-type=<sourceCatalogType>', "
                + "'--source-catalog-properties=<String=String>', '--target-catalog-type=<targetCatalogType>', "
                + "'--target-catalog-properties=<String=String>'"),
        // missing required arguments
        arguments(
            singletonList(""),
            "Missing required options: '--source-catalog-type=<sourceCatalogType>', "
                + "'--source-catalog-properties=<String=String>', '--target-catalog-type=<targetCatalogType>', "
                + "'--target-catalog-properties=<String=String>'"),
        // missing required arguments
        arguments(
            Arrays.asList("--source-catalog-type", "GLUE"),
            "Missing required options: '--source-catalog-properties=<String=String>', "
                + "'--target-catalog-type=<targetCatalogType>', '--target-catalog-properties=<String=String>'"),
        // missing required arguments
        arguments(
            Arrays.asList(
                "--source-catalog-type",
                "HIVE",
                "--source-catalog-properties",
                "properties1=ab",
                "--target-catalog-type",
                "NESSIE"),
            "Missing required option: '--target-catalog-properties=<String=String>'"),
        // missing required arguments
        arguments(
            Arrays.asList(
                "--source-catalog-type",
                "HIVE",
                "--source-catalog-properties",
                "properties1=ab",
                "--target-catalog-properties",
                "properties2=cd"),
            "Missing required option: '--target-catalog-type=<targetCatalogType>'"));
  }

  @ParameterizedTest
  @MethodSource("optionErrors")
  @Order(0)
  public void testOptionErrors(List<String> args, String expectedMessage) throws Exception {
    RunCLI run = RunCLI.run(args);

    Assertions.assertThat(run.getExitCode()).isEqualTo(2);
    Assertions.assertThat(run.getErr()).contains(expectedMessage);
  }

  @Test
  @Order(1)
  public void testInvalidArgs() throws Exception {
    RunCLI run =
        RunCLI.run(
            "--source-catalog-type",
            "HADOOP",
            "--source-catalog-properties",
            "k1=v1,k2=v2",
            "--target-catalog-type",
            "HIVE",
            "--target-catalog-properties",
            "k3=v3, k4=v4");
    Assertions.assertThat(run.getExitCode()).isEqualTo(1);
    Assertions.assertThat(run.getErr())
        .contains(
            "java.lang.IllegalArgumentException: Cannot initialize HadoopCatalog "
                + "because warehousePath must not be null or empty");

    run =
        RunCLI.run(
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
            "^foo\\.");
    Assertions.assertThat(run.getExitCode()).isEqualTo(1);
    Assertions.assertThat(run.getErr())
        .contains(
            "java.lang.IllegalArgumentException: All the three identifier options (`--identifiers`, "
                + "`--identifiers-from-file`, `--identifiers-regex`) are configured. Please use only one of them.");

    run =
        RunCLI.run(
            "--source-catalog-type",
            "HADOOP",
            "--source-catalog-properties",
            "k1=v1,k2=v2",
            "--target-catalog-type",
            "HIVE",
            "--target-catalog-properties",
            "k3=v3, k4=v4",
            "--identifiers-from-file",
            "file.txt");
    Assertions.assertThat(run.getExitCode()).isEqualTo(1);
    Assertions.assertThat(run.getErr())
        .contains(
            "java.lang.IllegalArgumentException: "
                + "File specified in `--identifiers-from-file` option does not exist.");

    run =
        RunCLI.run(
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
            "file.txt");
    Assertions.assertThat(run.getExitCode()).isEqualTo(1);
    Assertions.assertThat(run.getErr())
        .contains(
            "java.lang.IllegalArgumentException: Both `--identifiers` and `--identifiers-from-file` "
                + "options are configured. Please use only one of them.");

    run =
        RunCLI.run(
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
            "file.txt");
    Assertions.assertThat(run.getExitCode()).isEqualTo(1);
    Assertions.assertThat(run.getErr())
        .contains(
            "java.lang.IllegalArgumentException: Both `--identifiers-regex` "
                + "and `--identifiers-from-file` options are configured. Please use only one of them.");

    run =
        RunCLI.run(
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
            "^foo\\.");
    Assertions.assertThat(run.getExitCode()).isEqualTo(1);
    Assertions.assertThat(run.getErr())
        .contains(
            "java.lang.IllegalArgumentException: Both `--identifiers-regex` and "
                + "`--identifiers` options are configured. Please use only one of them.");
  }
}
