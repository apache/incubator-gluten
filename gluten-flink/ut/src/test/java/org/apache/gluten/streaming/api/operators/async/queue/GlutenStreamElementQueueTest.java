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

package org.apache.gluten.streaming.api.operators.async.queue;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueueTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class GlutenStreamElementQueueTest extends StreamElementQueueTest {

    private final AsyncDataStream.OutputMode outputMode;
    public  GlutenStreamElementQueueTest(AsyncDataStream.OutputMode outputMode) {
        super(outputMode);
        this.outputMode = outputMode;
    }

    static Stream<Object> parameters() {
        return Stream.of(
                new Object[] {AsyncDataStream.OutputMode.ORDERED}
        );
    }

    @ParameterizedTest
    @MethodSource("parameters")
    @Disabled
    public void testPut() {
    }

    @ParameterizedTest
    @MethodSource("parameters")
    @Disabled
    public void testPop() {
    }

    @ParameterizedTest
    @MethodSource("parameters")
    @Disabled
    public void testPutOnFull() throws Exception {
    }

    @ParameterizedTest
    @MethodSource("parameters")
    @Disabled
    public void testWatermarkOnly() {
    }

}
