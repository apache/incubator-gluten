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

package org.apache.gluten.streaming.api.operators.co;

import org.apache.flink.streaming.api.operators.co.IntervalJoinOperatorTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class GlutenIntervalJoinOperatorTest extends IntervalJoinOperatorTest {

    public GlutenIntervalJoinOperatorTest(boolean lhsFasterThanRhs) {
        super(lhsFasterThanRhs);
    }

    @Override
    @Test
    @Disabled
    public void testImplementationMirrorsCorrectly() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testNegativeExclusiveAndNegativeExlusive() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testNegativeExclusiveAndPositiveExlusive() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testPositiveExclusiveAndPositiveExlusive() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testStateCleanupNegativeInclusiveNegativeInclusive() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testStateCleanupNegativePositiveNegativeExlusive() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testStateCleanupPositiveInclusivePositiveInclusive() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testStateCleanupPositiveExlusivePositiveExclusive() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testRestoreFromSnapshot() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testContextCorrectLeftTimestamp() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testReturnsCorrectTimestamp() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testContextCorrectRightTimestamp() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testFailsWithNoTimestampsLeft() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testFailsWithNoTimestampsRight() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testDiscardsLateData() throws Exception {
    }

    @Override
    @Test
    @Disabled
    public void testLateData() throws Exception {
    }

}
