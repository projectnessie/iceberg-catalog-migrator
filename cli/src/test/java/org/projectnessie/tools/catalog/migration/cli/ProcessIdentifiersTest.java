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

import com.google.common.collect.Sets;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class ProcessIdentifiersTest {

  protected static @TempDir Path tempDir;

  @BeforeAll
  protected static void initLogDir() {
    System.setProperty("catalog.migration.log.dir", tempDir.toAbsolutePath().toString());
  }

  @Test
  public void testIdentifiersSet() {
    // test empty set
    Assertions.assertThat(new IdentifierOptions().processIdentifiersInput()).isEmpty();

    // test valid elements
    IdentifierOptions identifierOptions = new IdentifierOptions();
    identifierOptions.identifiers = Sets.newHashSet("foo.abc", "bar.def");
    Assertions.assertThat(identifierOptions.processIdentifiersInput())
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("foo.abc"), TableIdentifier.parse("bar.def"));
  }

  @Test
  public void testIdentifiersFromFile() throws Exception {
    // valid file contents
    Path identifierFile = tempDir.resolve("file_with_ids.txt");
    Files.write(identifierFile, Arrays.asList("db1.t1", "db2.t2", "db123.t5"));
    IdentifierOptions options = new IdentifierOptions();
    options.identifiersFromFile = identifierFile.toAbsolutePath().toString();
    Assertions.assertThat(options.processIdentifiersInput())
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("db1.t1"),
            TableIdentifier.parse("db2.t2"),
            TableIdentifier.parse("db123.t5"));

    // empty file
    identifierFile = tempDir.resolve("ids1.txt");
    Files.createFile(identifierFile);
    options = new IdentifierOptions();
    options.identifiersFromFile = identifierFile.toAbsolutePath().toString();
    Assertions.assertThat(options.processIdentifiersInput()).isEmpty();

    // file with some blanks contents
    identifierFile = tempDir.resolve("ids2.txt");
    String[] lines = {"abc. def", "    abc 123 ", "", "", "    xyz%n123"};
    Files.writeString(identifierFile, String.join(System.lineSeparator(), lines));
    options = new IdentifierOptions();
    options.identifiersFromFile = identifierFile.toAbsolutePath().toString();
    Set<TableIdentifier> identifiers = options.processIdentifiersInput();
    Assertions.assertThat(identifiers)
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("abc. def"),
            TableIdentifier.parse("abc 123"),
            TableIdentifier.parse("xyz%n123"));

    // with duplicate entries
    identifierFile = tempDir.resolve("ids3.txt");
    String[] ids = {"abc.def", "xx.yy", "abc.def", "abc.def", "abc.def ", " xx.yy"};
    Files.writeString(identifierFile, String.join(System.lineSeparator(), ids));
    options = new IdentifierOptions();
    options.identifiersFromFile = identifierFile.toAbsolutePath().toString();
    identifiers = options.processIdentifiersInput();
    Assertions.assertThat(identifiers)
        .containsExactlyInAnyOrder(
            TableIdentifier.parse("abc.def"), TableIdentifier.parse("xx.yy"));
  }

  @Test
  public void testIdentifiersFromFileInvalidInputs() throws Exception {
    // file without permission to read
    Path identifierFile = tempDir.resolve("non_readable_file.txt");
    Files.createFile(identifierFile);
    Assertions.assertThat(identifierFile.toFile().setReadable(false)).isTrue();
    IdentifierOptions options = new IdentifierOptions();
    options.identifiersFromFile = identifierFile.toAbsolutePath().toString();
    Assertions.assertThatThrownBy(options::processIdentifiersInput)
        .isInstanceOf(UncheckedIOException.class)
        .hasMessageContaining("Failed to read the file: " + identifierFile);
    Assertions.assertThat(identifierFile.toFile().setReadable(true)).isTrue();

    // file doesn't exist
    options.identifiersFromFile = "path/to/file";
    Assertions.assertThatThrownBy(options::processIdentifiersInput)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("File specified in `--identifiers-from-file` option does not exist");
  }

  @Test
  public void testIdentifiersRegEx() {
    // test valid regex
    IdentifierOptions options = new IdentifierOptions();
    options.identifiersRegEx = "^foo\\..*";
    Assertions.assertThat(options.processIdentifiersInput()).isEmpty();

    // test invalid regex
    options.identifiersRegEx = "(23erf423!";
    Assertions.assertThatThrownBy(options::processIdentifiersInput)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--identifiers-regex pattern is not compilable");

    options = new IdentifierOptions();
    options.identifiersRegEx = "  ";
    Assertions.assertThatThrownBy(options::processIdentifiersInput)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--identifiers-regex should not be empty");
  }
}
