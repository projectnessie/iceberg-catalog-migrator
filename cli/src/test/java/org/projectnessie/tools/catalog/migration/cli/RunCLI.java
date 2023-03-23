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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import nl.altindag.log.LogCaptor;
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

  public static RunCLI run(String... args) throws Exception {
    try (LogCaptor logCaptor = LogCaptor.forName("console-log");
        StringWriter err = new StringWriter();
        PrintWriter errWriter = new PrintWriter(err)) {
      int exitCode = runMain(null, errWriter, args);
      String out = String.join(System.lineSeparator(), logCaptor.getLogs());
      return new RunCLI(exitCode, out, err.toString(), args);
    }
  }

  public static RunCLI runWithPrintWriter(String... args) throws Exception {
    try (StringWriter out = new StringWriter();
        PrintWriter outWriter = new PrintWriter(out);
        StringWriter err = new StringWriter();
        PrintWriter errWriter = new PrintWriter(err)) {
      int exitCode = runMain(outWriter, errWriter, args);
      return new RunCLI(exitCode, out.toString(), err.toString(), args);
    }
  }

  private static int runMain(PrintWriter out, PrintWriter err, String... arguments) {
    CommandLine commandLine =
        new CommandLine(new CatalogMigrationCLI())
            .setExecutionExceptionHandler(
                (ex, cmd, parseResult) -> {
                  if (enableStacktrace(arguments)) {
                    cmd.getErr().println(cmd.getColorScheme().richStackTraceString(ex));
                  } else {
                    cmd.getErr().println("Error during CLI execution: " + ex.getMessage());
                  }
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
      commandLine.getErr().flush();
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

  private static boolean enableStacktrace(String... args) {
    for (String arg : args) {
      if (arg.equalsIgnoreCase("--stacktrace")) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format(
        "org.projectnessie.tools.catalog.migration.cli"
            + ".RunCLI{args=%s%nexitCode=%d%n%nstdout:%n%s%n%nstderr:%n%s",
        Arrays.toString(args), exitCode, out, err);
  }
}
