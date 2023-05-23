package io.glutenproject.integration.tpc.command;

import io.glutenproject.integration.tpc.TpcMixin;
import org.apache.commons.lang3.ArrayUtils;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "spark-shell", mixinStandardHelpOptions = true,
    showDefaultValues = true,
    description = "Open a standard Spark shell.")
public class SparkShell implements Callable<Integer> {
  @CommandLine.Mixin
  private TpcMixin mixin;

  @CommandLine.Mixin
  private DataGenMixin dataGenMixin;

  @Override
  public Integer call() throws Exception {
    io.glutenproject.integration.tpc.action.SparkShell sparkShell =
        new io.glutenproject.integration.tpc.action.SparkShell(dataGenMixin.getScale());
    return mixin.runActions(ArrayUtils.addAll(dataGenMixin.makeActions(), sparkShell));
  }
}
