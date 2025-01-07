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
package org.apache.gluten.integration.command;

import com.google.common.base.Preconditions;
import org.apache.gluten.integration.BaseMixin;
import org.apache.commons.lang3.ArrayUtils;
import picocli.CommandLine;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@CommandLine.Command(name = "parameterized", mixinStandardHelpOptions = true,
    showDefaultValues = true,
    description = "Run queries with parameterized configurations")
public class Parameterized implements Callable<Integer> {
  @CommandLine.Mixin
  private BaseMixin mixin;

  @CommandLine.Mixin
  private DataGenMixin dataGenMixin;

  @CommandLine.Mixin
  private QueriesMixin queriesMixin;

  @CommandLine.Option(names = {"--warmup-iterations"}, description = "Dry-run iterations before actually run the test", defaultValue = "0")
  private int warmupIterations;

  @CommandLine.Option(names = {"-m", "--metric"}, description = "Specify a series of executor metrics to collect during execution")
  private String[] metrics = new String[0];

  @CommandLine.Option(names = {"-d", "--dim"}, description = "Set a series of dimensions consisting of possible config options, example: -d=offheap:1g,spark.memory.offHeap.enabled=true,spark.memory.offHeap.size=1g")
  private String[] dims = new String[0];

  @CommandLine.Option(names = {"--excluded-dims"}, description = "Set a series of comma-separated list of dimension combinations to exclude. Example: --exclude-dims=offheap:1g,aqe=on")
  private String[] excludedDims = new String[0];

  private static final Pattern dimPattern1 = Pattern.compile("([\\w-]+):([^,:]+)((?:,[^=,]+=[^=,]+)+)");
  private static final Pattern dimPattern2 = Pattern.compile("([^,:]+)((?:,[^=,]+=[^=,]+)+)");

  private static final Pattern excludedDimsPattern = Pattern.compile("[\\w-]+:[^,:]+(?:,[\\w-]+:[^,:]+)*");

  @Override
  public Integer call() throws Exception {
    final Map<String, Map<String, List<Map.Entry<String, String>>>> parsed = new LinkedHashMap<>();

    final Seq<scala.collection.immutable.Set<org.apache.gluten.integration.action.Parameterized.DimKv>> excludedCombinations = JavaConverters.asScalaBufferConverter(Arrays.stream(excludedDims).map(d -> {
      final Matcher m = excludedDimsPattern.matcher(d);
      Preconditions.checkArgument(m.matches(), "Unrecognizable excluded dims: " + d);
      Set<org.apache.gluten.integration.action.Parameterized.DimKv> out = new HashSet<>();
      final String[] dims = d.split(",");
      for (String dim : dims) {
        final String[] kv = dim.split(":");
        Preconditions.checkArgument(kv.length == 2, "Unrecognizable excluded dims: " + d);
        out.add(new org.apache.gluten.integration.action.Parameterized.DimKv(kv[0], kv[1]));
      }
      return JavaConverters.asScalaSetConverter(out).asScala().<org.apache.gluten.integration.action.Parameterized.DimKv>toSet();
    }).collect(Collectors.toList())).asScala();

    // parse dims
    for (String dim : dims) {
      Matcher matcher1 = dimPattern1.matcher(dim);
      Matcher matcher2 = dimPattern2.matcher(dim);
      if (matcher1.matches() == matcher2.matches()) {
        throw new IllegalArgumentException("Unexpected dim: " + dim);
      }
      final String dimName;
      final String dimValueName;
      final String confText;

      if (matcher1.matches()) {
        dimName = matcher1.group(1);
        dimValueName = matcher1.group(2);
        confText = matcher1.group(3).substring(1); // trim leading ","
      } else {
        // matcher2.matches
        dimName = matcher2.group(1);
        dimValueName = matcher2.group(0);
        confText = matcher2.group(2).substring(1); // trim leading ","
      }

      final List<Map.Entry<String, String>> options = new ArrayList<>();
      String[] splits = confText.split(",");
      if (splits.length == 0) {
        throw new IllegalArgumentException("Unexpected dim: " + dim);
      }
      for (String split : splits) {
        String[] kv = split.split("=");
        if (kv.length != 2) {
          throw new IllegalArgumentException("Unexpected dim: " + dim);
        }
        options.add(new AbstractMap.SimpleImmutableEntry<>(kv[0], kv[1]));
      }

      parsed.computeIfAbsent(dimName, s -> new LinkedHashMap<>())
          .computeIfAbsent(dimValueName, s -> new ArrayList<>())
          .addAll(options);
    }

    // Convert Map<String, Map<String, List<Map.Entry<String, String>>>> to List<Dim>
    Seq<org.apache.gluten.integration.action.Parameterized.Dim> parsedDims = JavaConverters.asScalaBufferConverter(
        parsed.entrySet().stream().map(e ->
            new org.apache.gluten.integration.action.Parameterized.Dim(e.getKey(), JavaConverters.asScalaBufferConverter(
                e.getValue().entrySet().stream().map(e2 ->
                    new org.apache.gluten.integration.action.Parameterized.DimValue(e2.getKey(), JavaConverters.asScalaBufferConverter(
                        e2.getValue().stream().map(e3 -> new Tuple2<>(e3.getKey(), e3.getValue()))
                            .collect(Collectors.toList())).asScala())).collect(Collectors.toList())).asScala()
            )).collect(Collectors.toList())).asScala();

    org.apache.gluten.integration.action.Parameterized parameterized =
        new org.apache.gluten.integration.action.Parameterized(dataGenMixin.getScale(),
            dataGenMixin.genPartitionedData(), queriesMixin.queries(),
            queriesMixin.explain(), queriesMixin.iterations(), warmupIterations, queriesMixin.noSessionReuse(), parsedDims,
            excludedCombinations, JavaConverters.asScalaBufferConverter(Arrays.asList(metrics)).asScala());
    return mixin.runActions(ArrayUtils.addAll(dataGenMixin.makeActions(), parameterized));
  }
}
