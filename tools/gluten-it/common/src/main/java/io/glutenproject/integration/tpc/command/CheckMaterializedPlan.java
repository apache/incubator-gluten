package io.glutenproject.integration.tpc.command;

import io.glutenproject.integration.tpc.TpcMixin;
import org.apache.commons.lang3.ArrayUtils;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "check-materialized-plan", mixinStandardHelpOptions = true,
        showDefaultValues = true,
        description = "Run queries and check whether spark plan changed.")
public class CheckMaterializedPlan implements Callable<Integer> {
    @CommandLine.Mixin
    private TpcMixin mixin;

    @CommandLine.Mixin
    private DataGenMixin dataGenMixin;

    @CommandLine.Option(names = {"--queries"}, description = "Set a comma-separated list of query IDs to run, run all queries if not specified. Example: --queries=q1,q6", split = ",")
    private String[] queries = new String[0];

    @CommandLine.Option(names = {"--gen-golden-file"}, description = "Generate current spark plan file", defaultValue = "false")
    private boolean genGoldenFile;

    @Override
    public Integer call() throws Exception {
        io.glutenproject.integration.tpc.action.CheckMaterializedPlan checkMaterializedPlan =
            new io.glutenproject.integration.tpc.action.CheckMaterializedPlan(dataGenMixin.getScale(), queries, genGoldenFile);
        return mixin.runActions(ArrayUtils.addAll(dataGenMixin.makeActions(), checkMaterializedPlan));
    }
}
