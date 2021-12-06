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

package com.intel.oap.spark.sql.execution.datasources.v2.arrow;

import org.apache.arrow.memory.AllocationListener;

public class SparkManagedAllocationListener implements AllocationListener {

    private final NativeSQLMemoryConsumer consumer;
    private final NativeSQLMemoryMetrics metrics;

    public SparkManagedAllocationListener(NativeSQLMemoryConsumer consumer, NativeSQLMemoryMetrics metrics) {
        this.consumer = consumer;
        this.metrics = metrics;
    }

    @Override
    public void onPreAllocation(long size) {
        consumer.acquire(size);
        metrics.inc(size);
    }

    @Override
    public void onRelease(long size) {
        consumer.free(size);
        metrics.inc(-size);
    }
}
