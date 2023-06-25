package io.glutenproject.integration.tpc;

import io.glutenproject.integration.tpc.command.DataGenOnly;
import io.glutenproject.integration.tpc.command.Parameterized;
import io.glutenproject.integration.tpc.command.Queries;
import io.glutenproject.integration.tpc.command.QueriesCompare;
import io.glutenproject.integration.tpc.command.SparkShell;
import picocli.CommandLine;

@CommandLine.Command(name = "gluten-it", mixinStandardHelpOptions = true,
    showDefaultValues = true,
    subcommands = {DataGenOnly.class, Queries.class, QueriesCompare.class, SparkShell.class, Parameterized.class},
    description = "Gluten integration test using TPC benchmark's data and queries.")
public class Tpc {

  private Tpc() {
  }

  public static void main(String... args) {
    final CommandLine cmd = new CommandLine(new Tpc());
    final int exitCode = cmd.execute(args);
    System.exit(exitCode);
  }
}
