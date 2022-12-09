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
import org.junit.jupiter.api.Assertions;
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
            "Missing required parameters: '<sourceCatalogType>', '<targetCatalogType>'"),
        // invalid argument
        arguments(
            singletonList(""),
            "Invalid value for positional parameter at index 0 (<sourceCatalogType>): "
                + "expected one of [CUSTOM, DYNAMODB, ECS, GLUE, HADOOP, HIVE, JDBC, NESSIE, REST] (case-sensitive)"),
        // missing required arguments
        arguments(singletonList("GLUE"), "Missing required parameter: '<targetCatalogType>'"),
        // invalid argument
        arguments(
            Arrays.asList("HIVE", "properties1=ab", "properties2=cd"),
            "Invalid value for positional parameter at index 2 (<targetCatalogType>): expected one of [CUSTOM, "
                + "DYNAMODB, ECS, GLUE, HADOOP, HIVE, JDBC, NESSIE, REST] (case-sensitive) but was 'properties2=cd'"));
  }

  @ParameterizedTest
  @MethodSource("optionErrors")
  @Order(0)
  public void testOptionErrors(List<String> args, String expectedMessage) throws Exception {
    RunCLI run = RunCLI.run(args);

    Assertions.assertEquals(2, run.getExitCode());
    Assertions.assertTrue(run.getErr().contains(expectedMessage));
  }

  @Test
  @Order(1)
  public void testInvalidArgs() throws Exception {
    RunCLI run = RunCLI.run("HADOOP", "k1=v1,k2=v2", "HIVE", "k3=v3, k4=v4");
    Assertions.assertEquals(1, run.getExitCode());
    Assertions.assertTrue(
        run.getErr()
            .contains(
                "java.lang.IllegalArgumentException: Cannot initialize HadoopCatalog "
                    + "because warehousePath must not be null or empty"));

    run =
        RunCLI.run(
            "HADOOP",
            "k1=v1,k2=v2",
            "HIVE",
            "k3=v3, k4=v4",
            "-I",
            "foo.tbl",
            "--identifiers-from-file",
            "file.txt");
    Assertions.assertEquals(1, run.getExitCode());
    Assertions.assertTrue(
        run.getErr()
            .contains(
                "java.lang.IllegalArgumentException: Both `--identifiers` and `--identifiers-from-file` "
                    + "options are configured. Please use only one of them."));

    run =
        RunCLI.run(
            "HADOOP", "k1=v1,k2=v2", "HIVE", "k3=v3, k4=v4", "--identifiers-from-file", "file.txt");
    Assertions.assertEquals(1, run.getExitCode());
    Assertions.assertTrue(
        run.getErr()
            .contains(
                "java.lang.IllegalArgumentException: File specified in `--identifiers-from-file` option does not exist."));
  }
}
