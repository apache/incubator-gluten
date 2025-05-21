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

package org.apache.gluten.streaming.api.operators.async;

import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class GlutenAsyncWaitOperatorTest extends AsyncWaitOperatorTest {

    @Override
    @Test
    @Disabled
    public void testEventTimeOrdered() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testWaterMarkUnordered() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testProcessingTimeOrdered() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testProcessingUnordered() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testOperatorChainWithProcessingTime() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testStateSnapshotAndRestore() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testObjectReused() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testAsyncTimeoutFailure() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testAsyncTimeoutIgnore() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testTimeoutCleanup() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testTimeoutAfterComplete() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testOrderedWaitUserExceptionHandling() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testOrderedWaitUserExceptionHandlingWithRetry() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testUnorderedWaitUserExceptionHandling() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testUnorderedWaitUserExceptionHandlingWithRetry() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testOrderedWaitTimeoutHandling() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testOrderedWaitTimeoutHandlingWithRetry() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testUnorderedWaitTimeoutHandling() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testUnorderedWaitTimeoutHandlingWithRetry() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testRestartWithFullQueue() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testIgnoreAsyncOperatorRecordsOnDrain() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testIgnoreAsyncOperatorRecordsOnDrainWithRetry() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testProcessingTimeOrderedWithRetry() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testProcessingTimeUnorderedWithRetry() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testProcessingTimeRepeatedCompleteUnorderedWithRetry() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testProcessingTimeRepeatedCompleteOrderedWithRetry() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testProcessingTimeWithTimeoutFunctionUnorderedWithRetry() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testProcessingTimeWithTimeoutFunctionOrderedWithRetry() throws Exception {
    }

}
