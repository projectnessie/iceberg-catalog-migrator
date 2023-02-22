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

import static org.mockito.Mockito.mockStatic;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.mockito.MockedStatic;
import org.projectnessie.tools.catalog.migration.cli.CatalogMigrationCLI;
import org.projectnessie.tools.catalog.migration.cli.PromptUtil;
import picocli.CommandLine;

/** Helper class for tests. */
public final class RunCLI {

  private final String[] args;
  private final int exitCode;
  private final String out;
  private final String err;

  public RunCLI(int exitCode, String out, String err, String[] args) {
    this.args = args;
    this.exitCode = exitCode;
    this.out = out;
    this.err = err;
  }

  public static RunCLI run(List<String> args) throws Exception {
    return run(args.toArray(new String[0]));
  }

  private static int runMain(PrintWriter out, PrintWriter err, String... arguments) {
    CommandLine commandLine =
        new CommandLine(new CatalogMigrationCLI())
            .setExecutionExceptionHandler(
                (ex, cmd, parseResult) -> {
                  cmd.getErr().println(cmd.getColorScheme().richStackTraceString(ex));
                  return cmd.getExitCodeExceptionMapper() != null
                      ? cmd.getExitCodeExceptionMapper().getExitCode(ex)
                      : cmd.getCommandSpec().exitCodeOnExecutionException();
                });
    if (null != out) {
      commandLine = commandLine.setOut(out);
    }
    if (null != err) {
      commandLine = commandLine.setErr(err);
    }
    try {
      return commandLine.execute(arguments);
    } finally {
      commandLine.getOut().flush();
      commandLine.getErr().flush();
    }
  }

  public static RunCLI run(String... args) throws Exception {
    try (StringWriter out = new StringWriter();
        PrintWriter outWriter = new PrintWriter(out);
        StringWriter err = new StringWriter();
        PrintWriter errWriter = new PrintWriter(err)) {
      int exitCode = runMain(outWriter, errWriter, args);
      return new RunCLI(exitCode, out.toString(), err.toString(), args);
    }
  }

  static RunCLI runWithMockedPrompts(String... args) throws Exception {
    try (StringWriter out = new StringWriter();
        PrintWriter outWriter = new PrintWriter(out);
        StringWriter err = new StringWriter();
        PrintWriter errWriter = new PrintWriter(err)) {

      AtomicInteger exitCode = new AtomicInteger();
      try (MockedStatic<PromptUtil> mocked = mockStatic(PromptUtil.class)) {

        // To avoid manipulating `System.in`, mock the APIs that use `System.in`
        mocked.when(() -> PromptUtil.proceedForMigration(outWriter)).thenReturn(true);
        mocked.when(() -> PromptUtil.proceedForRegistration(outWriter)).thenReturn(true);

        exitCode.set(runMain(outWriter, errWriter, args));
        return new RunCLI(exitCode.get(), out.toString(), err.toString(), args);
      }
    }
  }

  public int getExitCode() {
    return exitCode;
  }

  public String getOut() {
    return out;
  }

  public String getErr() {
    return err;
  }

  @Override
  public String toString() {
    return String.format(
        "org.projectnessie.tools.catalog.migration"
            + ".RunCLI{args=%s%nexitCode=%d%n%nstdout:%n%s%n%nstderr:%n%s",
        Arrays.toString(args), exitCode, out, err);
  }
}
