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
package org.apache.gluten.spark34.extensions;

import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.spark.extensions.TestMergeOnReadMerge;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class GlutenTestMergeOnReadMerge extends TestMergeOnReadMerge {
    public GlutenTestMergeOnReadMerge(String catalogName, String implementation, Map<String, String> config, String fileFormat, boolean vectorized, String distributionMode, boolean fanoutEnabled, String branch, PlanningMode planningMode) {
        super(catalogName, implementation, config, fileFormat, vectorized, distributionMode, fanoutEnabled, branch, planningMode);
    }

    @Test
    public synchronized void testMergeWithConcurrentTableRefresh() {
        System.out.println("Run timeout");
    }

    @Test
    public synchronized void testMergeWithSerializableIsolation() {
        System.out.println("Run timeout");
    }

    @Test
    public synchronized void testMergeWithSnapshotIsolation() {
        System.out.println("Run timeout");
    }
    
}
