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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.shaded.org.checkerframework.checker.signature.qual.MethodDescriptor;

import java.util.stream.Stream;

public class GlutenInternalTimerServiceImplTest extends InternalTimerServiceImplTest {

    private final int startKeyGroup;
    private final int endKeyGroup;
    private final int maxParallelism;
	public GlutenInternalTimerServiceImplTest(int startKeyGroup, int endKeyGroup, int maxParallelism) {
        super(startKeyGroup, endKeyGroup, maxParallelism);
        this.startKeyGroup = startKeyGroup;
        this.endKeyGroup = endKeyGroup;
        this.maxParallelism = maxParallelism;
    }

    static Stream<Object> parameters() {
        return Stream.of(
                new Object[] {0, 10, 128}
        );
    }

    @ParameterizedTest
    @MethodSource("parameters")
    @Disabled
    public void testKeyGroupStartIndexSetting() {
    }

    @ParameterizedTest
    @MethodSource("parameters")
    @Disabled
    public void testTimerAssignmentToKeyGroups() {
    }

    @ParameterizedTest
    @MethodSource("parameters")
    @Disabled
    public void testOnlySetsOnePhysicalProcessingTimeTimer() throws Exception {
    }

    @ParameterizedTest
    @MethodSource("parameters")
    @Disabled
    public void testRegisterEarlierProcessingTimerMovesPhysicalProcessingTimer() throws Exception {
    }

    @ParameterizedTest
    @MethodSource("parameters")
    @Disabled
    public void testRegisteringProcessingTimeTimerInOnProcessingTimeDoesNotLeakPhysicalTimers() throws Exception {
    }

    @ParameterizedTest
    @MethodSource("parameters")
    @Disabled
    public void testCurrentProcessingTime() throws Exception {
    }

    @ParameterizedTest
    @MethodSource("parameters")
    @Disabled
    public void testCurrentEventTime() throws Exception {
    }

    @ParameterizedTest
    @MethodSource("parameters")
    @Disabled
    public void testSetAndFireEventTimeTimers() throws Exception {
    }

    @ParameterizedTest
    @MethodSource("parameters")
    @Disabled
    public void testSetAndFireProcessingTimeTimers() throws Exception {
    }

    @ParameterizedTest
    @MethodSource("parameters")
    @Disabled
    public void testDeleteEventTimeTimers() throws Exception {
    }

    @ParameterizedTest
    @MethodSource("parameters")
    @Disabled
    public void testDeleteProcessingTimeTimers() throws Exception {
    }

    @ParameterizedTest
    @MethodSource("parameters")
    @Disabled
    public void testForEachEventTimeTimers() throws Exception {
    }

    @ParameterizedTest
    @MethodSource("parameters")
    @Disabled
    public void testForEachProcessingTimeTimers() throws Exception {
    }

    @ParameterizedTest
    @MethodSource("parameters")
    @Disabled
    public void testSnapshotAndRestore() throws Exception {
    }

    @ParameterizedTest
    @MethodSource("parameters")
    @Disabled
    public void testSnapshotAndRebalancingRestore() throws Exception {
    }

}
