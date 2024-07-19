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
import org.apache.gluten.integration.Suite;
import org.apache.gluten.integration.action.Actions;
import picocli.CommandLine;
import scala.collection.Seq;
import scala.collection.JavaConverters;

import java.util.*;
import java.util.stream.Collectors;

public class QueriesMixin {
  @CommandLine.Option(names = {"--queries"}, description = "Set a comma-separated list of query IDs to run, run all queries if not specified. Example: --queries=q1,q6", split = ",")
  private String[] queries = new String[0];

  @CommandLine.Option(names = {"--excluded-queries"}, description = "Set a comma-separated list of query IDs to exclude. Example: --exclude-queries=q1,q6", split = ",")
  private String[] excludedQueries = new String[0];

  @CommandLine.Option(names = {"--shard"}, description = "Divide the queries to execute into N shards, then pick one single shard and run it. Example: --shard=1/3", defaultValue = "1/1")
  private String shard;

  @CommandLine.Option(names = {"--explain"}, description = "Output explain result for queries", defaultValue = "false")
  private boolean explain;

  @CommandLine.Option(names = {"--iterations"}, description = "How many iterations to run", defaultValue = "1")
  private int iterations;

  @CommandLine.Option(names = {"--no-session-reuse"}, description = "Recreate new Spark session each time a query is about to run", defaultValue = "false")
  private boolean noSessionReuse;

  public boolean explain() {
    return explain;
  }

  public int iterations() {
    return iterations;
  }

  public boolean noSessionReuse() {
    return noSessionReuse;
  }

  public Actions.QuerySelector queries() {
    return new Actions.QuerySelector() {
      @Override
      public Seq<String> select(Suite suite) {
        final List<String> all = select0(suite);
        final Division div = Division.parse(shard);
        final List<String> out = div(all, div);
        System.out.println("About to run queries: " + out + "... ");
        return JavaConverters.asScalaBuffer(out);
      }

      private List<String> div(List<String> from, Division div) {
        final int queryCount = from.size();
        final int shardCount = div.shardCount;
        final int least = queryCount / shardCount;
        final int shardIdx = div.shard - 1;
        final int shardStart = shardIdx * least;
        final int numQueriesInShard;
        if (shardIdx == shardCount - 1) {
          final int remaining = queryCount - least * shardCount;
          numQueriesInShard = least + remaining;
        } else {
          numQueriesInShard = least;
        }
        final List<String> out = new ArrayList<>();
        for (int i = shardStart; i < shardStart + numQueriesInShard; i++) {
          out.add(from.get(i));
        }
        return out;
      }

      private List<String> select0(Suite suite) {
        final String[] queryIds = queries;
        final String[] excludedQueryIds = excludedQueries;
        if (queryIds.length > 0 && excludedQueryIds.length > 0) {
          throw new IllegalArgumentException(
              "Should not specify queries and excluded queries at the same time");
        }
        String[] all = suite.allQueryIds();
        Set<String> allSet = new HashSet<>(Arrays.asList(all));
        if (queryIds.length > 0) {
          for (String id : queryIds) {
            if (!allSet.contains(id)) {
              throw new IllegalArgumentException("Invalid query ID: " + id);
            }
          }
          return Arrays.asList(queryIds);
        }
        if (excludedQueryIds.length > 0) {
          for (String id : excludedQueryIds) {
            if (!allSet.contains(id)) {
              throw new IllegalArgumentException("Invalid query ID to exclude: " + id);
            }
          }
          Set<String> excludedSet = new HashSet<>(Arrays.asList(excludedQueryIds));
          return Arrays.stream(all)
              .filter(id -> !excludedSet.contains(id))
              .collect(Collectors.toList());
        }
        return Arrays.asList(all);
      }
    };
  }

  private static class Division {
    private final int shard;
    private final int shardCount;

    private Division(int shard, int shardCount) {
      this.shard = shard;
      this.shardCount = shardCount;
    }

    private static Division parse(String shard) {
      String[] parts = shard.split("/");
      if (parts.length != 2) {
        throw new IllegalArgumentException("Invalid shard: " + shard);
      }
      int s = Integer.parseInt(parts[0]);
      int c = Integer.parseInt(parts[1]);
      Preconditions.checkArgument(s >= 1 && c >= 1 && s <= c, "Invalid picked shard: " + shard);
      return new Division(s, c);
    }
  }
}
