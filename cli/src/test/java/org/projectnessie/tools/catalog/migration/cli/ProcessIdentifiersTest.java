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

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.iceberg.catalog.TableIdentifier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class ProcessIdentifiersTest {

  protected static @TempDir Path logDir;

  @BeforeAll
  protected static void initLogDir() {
    System.setProperty("catalog.migration.log.dir", logDir.toAbsolutePath().toString());
  }

  @Test
  public void testOptions() throws Exception {
    Assertions.assertThat(new IdentifierOptions().processIdentifiersInput()).isEmpty();

    IdentifierOptions identifierOptions = new IdentifierOptions();
    identifierOptions.identifiers = Arrays.asList("foo.abc", "bar.def");
    Assertions.assertThat(identifierOptions.processIdentifiersInput())
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.abc"), TableIdentifier.parse("bar.def"));

    Path identifierFile = logDir.resolve("file_with_ids.txt");
    Files.write(identifierFile, Arrays.asList("db1.t1", "db2.t2", "db123.t5"));
    IdentifierOptions newOptions = new IdentifierOptions();
    newOptions.identifiersFromFile = identifierFile.toAbsolutePath().toString();
    Assertions.assertThat(newOptions.processIdentifiersInput())
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("db1.t1"),
            TableIdentifier.parse("db2.t2"),
            TableIdentifier.parse("db123.t5"));

    identifierFile.toFile().setReadable(false);
    Assertions.assertThatThrownBy(newOptions::processIdentifiersInput)
        .isInstanceOf(UncheckedIOException.class)
        .hasMessageContaining("Failed to read the file: " + identifierFile);
    identifierFile.toFile().setReadable(true);

    IdentifierOptions options = new IdentifierOptions();
    options.identifiersFromFile = "path/to/file";
    Assertions.assertThatThrownBy(options::processIdentifiersInput)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("File specified in `--identifiers-from-file` option does not exist");
  }
}
