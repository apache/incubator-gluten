/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
