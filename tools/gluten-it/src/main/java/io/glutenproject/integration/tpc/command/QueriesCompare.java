package io.glutenproject.integration.tpc.command;

import io.glutenproject.integration.tpc.TpcMixin;
import org.apache.commons.lang3.ArrayUtils;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "queries-compare", mixinStandardHelpOptions = true,
    showDefaultValues = true,
    description = "Run queries and do result comparison with baseline preset.")
public class QueriesCompare implements Callable<Integer> {
    @CommandLine.Mixin
    private TpcMixin mixin;

    @CommandLine.Mixin
    private DataGenMixin dataGenMixin;

    @CommandLine.Option(names = {"--queries"}, description = "Set a comma-separated list of query IDs to run, run all queries if not specified. Example: --queries=q1,q6", split = ",")
    private String[] queries = new String[0];

    @CommandLine.Option(names = {"--explain"}, description = "Output explain result for queries", defaultValue = "false")
    private boolean explain;

    @CommandLine.Option(names = {"--iterations"}, description = "How many iterations to run", defaultValue = "1")
    private int iterations;

  @Override
  public Integer call() throws Exception {
    io.glutenproject.integration.tpc.action.QueriesCompare queriesCompare =
        new io.glutenproject.integration.tpc.action.QueriesCompare(dataGenMixin.getScale(), this.queries, explain, iterations);
    return mixin.runActions(ArrayUtils.addAll(dataGenMixin.makeActions(), queriesCompare));
  }
}
