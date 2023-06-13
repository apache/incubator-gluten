package io.glutenproject.integration.tpc.command;

import io.glutenproject.integration.tpc.action.Action;
import picocli.CommandLine;

public class DataGenMixin {
  @CommandLine.Option(names = {"-s", "--scale"}, description = "The scale factor of sample TPC-H dataset", defaultValue = "0.1")
  private double scale;

  @CommandLine.Option(names = {"--gen-partitioned-data"}, description = "Generate data with partitions", defaultValue = "false")
  private boolean genPartitionedData;

  @CommandLine.Option(names = {"--skip-data-gen"}, description = "Skip data generation", defaultValue = "false")
  private boolean skipDataGen;

  public Action[] makeActions() {
    if (skipDataGen) {
      return new Action[0];
    }
    return new Action[]{new io.glutenproject.integration.tpc.action.DataGenOnly(scale, genPartitionedData)};
  }

  public double getScale() {
    return scale;
  }
}
