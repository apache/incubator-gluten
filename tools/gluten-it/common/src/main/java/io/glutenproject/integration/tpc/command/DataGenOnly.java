package io.glutenproject.integration.tpc.command;

import io.glutenproject.integration.tpc.TpcMixin;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "data-gen-only", mixinStandardHelpOptions = true,
    showDefaultValues = true,
    description = "Generate data only.")
public class DataGenOnly implements Callable<Integer> {
  @CommandLine.Mixin
  private TpcMixin mixin;

  @CommandLine.Mixin
  private DataGenMixin dataGenMixin;

  @Override
  public Integer call() throws Exception {
    return mixin.runActions(dataGenMixin.makeActions());
  }
}
