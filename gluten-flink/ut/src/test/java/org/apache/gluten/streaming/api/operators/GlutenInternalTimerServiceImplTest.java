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

package org.apache.gluten.streaming.api.operators;

import org.apache.flink.streaming.api.operators.InternalTimerServiceImplTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class GlutenInternalTimerServiceImplTest extends InternalTimerServiceImplTest {

    public GlutenInternalTimerServiceImplTest(int startKeyGroup, int endKeyGroup, int maxParallelism) {
        super(startKeyGroup, endKeyGroup, maxParallelism);
    }

    @Override
    @Test
    @Disabled
    public void testKeyGroupStartIndexSetting() {
    }

    @Override
    @Test
    @Disabled
    public void testTimerAssignmentToKeyGroups() {
    }

    @Override
    @Test
    @Disabled
    public void testOnlySetsOnePhysicalProcessingTimeTimer() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testRegisterEarlierProcessingTimerMovesPhysicalProcessingTimer() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testRegisteringProcessingTimeTimerInOnProcessingTimeDoesNotLeakPhysicalTimers() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testCurrentProcessingTime() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testCurrentEventTime() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testSetAndFireEventTimeTimers() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testSetAndFireProcessingTimeTimers() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testDeleteEventTimeTimers() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testDeleteProcessingTimeTimers() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testForEachEventTimeTimers() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testForEachProcessingTimeTimers() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testSnapshotAndRestore() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testSnapshotAndRebalancingRestore() throws Exception {
    }

}
