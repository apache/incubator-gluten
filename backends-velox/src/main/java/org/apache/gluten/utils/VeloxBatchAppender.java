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
package org.apache.gluten.utils;

import org.apache.gluten.exec.Runtime;
import org.apache.gluten.exec.Runtimes;
import org.apache.gluten.memory.nmm.NativeMemoryManager;
import org.apache.gluten.memory.nmm.NativeMemoryManagers;
import org.apache.gluten.vectorized.ColumnarBatchInIterator;
import org.apache.gluten.vectorized.ColumnarBatchOutIterator;

import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.Iterator;

public final class VeloxBatchAppender {
  public static ColumnarBatchOutIterator create(
      int minOutputBatchSize, Iterator<ColumnarBatch> in) {
    final Runtime runtime = Runtimes.contextInstance();
    final NativeMemoryManager nmm = NativeMemoryManagers.contextInstance("VeloxBatchAppender");
    long outHandle =
        VeloxBatchAppenderJniWrapper.forRuntime(runtime)
            .create(
                nmm.getNativeInstanceHandle(), minOutputBatchSize, new ColumnarBatchInIterator(in));
    return new ColumnarBatchOutIterator(runtime, outHandle, nmm);
  }
}
