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

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import picocli.CommandLine;

public class IdentifierOptions {

  @CommandLine.Spec CommandLine.Model.CommandSpec commandSpec;

  @CommandLine.Option(
      names = {"--identifiers"},
      split = ",",
      description =
          "optional selective list of identifiers to register. If not specified, all the tables will be registered. "
              + "Use this when there are few identifiers that need to be registered. For a large number of identifiers, "
              + "use the `--identifiers-from-file` or `--identifiers-regex` option.")
  private List<String> identifiers = new ArrayList<>();

  @CommandLine.Option(
      names = {"--identifiers-from-file"},
      description =
          "optional text file path that contains a list of table identifiers (one per line) to register. Should not be "
              + "used with `--identifiers` or `--identifiers-regex` option.")
  private String identifiersFromFile;

  @CommandLine.Option(
      names = {"--identifiers-regex"},
      description =
          "optional regular expression pattern used to register only the tables whose identifiers match this pattern. "
              + "Should not be used with `--identifiers` or '--identifiers-from-file' option.")
  protected String identifiersRegEx;

  protected List<TableIdentifier> processIdentifiersInput() {
    if (identifiersFromFile != null && !Files.exists(Paths.get(identifiersFromFile))) {
      throw new IllegalArgumentException(
          "File specified in `--identifiers-from-file` option does not exist.");
    }
    PrintWriter printWriter = commandSpec.commandLine().getOut();
    List<TableIdentifier> tableIdentifiers;
    if (identifiersFromFile != null) {
      try {
        printWriter.printf("Collecting identifiers from the file %s...%n", identifiersFromFile);
        tableIdentifiers =
            Files.readAllLines(Paths.get(identifiersFromFile)).stream()
                .map(TableIdentifier::parse)
                .collect(Collectors.toList());
      } catch (IOException e) {
        throw new RuntimeException("Failed to read the file:", e);
      }
    } else if (!identifiers.isEmpty()) {
      tableIdentifiers =
          identifiers.stream().map(TableIdentifier::parse).collect(Collectors.toList());
    } else {
      tableIdentifiers = Collections.emptyList();
    }
    return tableIdentifiers;
  }
}
