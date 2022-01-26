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

package com.intel.oap.vectorized;

import com.intel.oap.execution.ColumnarNativeIterator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.spark.memory.MemoryConsumer;

import java.io.IOException;
import java.util.List;

/**
 * This class is implemented in JNI. This provides the Java interface to invoke
 * functions in JNI. This file is used to generated the .h files required for
 * jni. Avoid all external dependencies in this file.
 */
public class ExpressionEvaluatorJniWrapper {
        public String tmp_dir_path;

        /** Wrapper for native API. */
        public ExpressionEvaluatorJniWrapper(String tmp_dir, List<String> listJars, String libName,
                                             String libPath, boolean loadArrowAndGandiva)
                        throws IOException, IllegalAccessException, IllegalStateException {
                JniInstance jni = JniInstance.getInstance(tmp_dir, libName, libPath,
                        loadArrowAndGandiva);
                jni.setTempDir();
                jni.setJars(listJars);
                tmp_dir_path = jni.getTempDir();
        }

        /**
         * Call initNative to initialize native computing.
         */
        native void nativeInitNative();

        /**
         * Create a whole_stage_transform kernel, and return a result iterator.
         *
         * @param nativeHandler nativeHandler of this expression
         * @return iterator instance id
         */
        native long nativeCreateKernelWithIterator(long nativeHandler,
                                                   byte[] wsExprListBuf,
                                                   ColumnarNativeIterator[] batchItr) throws RuntimeException;

        /**
         * Create a whole stage transform kernel and return a row iterator.
         */
        native long nativeCreateKernelWithRowIterator(byte[] wsPlan) throws RuntimeException;

        /**
         * Set native env variables NATIVE_TMP_DIR
         *
         * @param path tmp path for native codes, use java.io.tmpdir
         */
        native void nativeSetJavaTmpDir(String path);

        /**
         * Set native env variables NATIVE_BATCH_SIZE
         *
         * @param batch_size numRows of one batch, use
         *                   spark.sql.execution.arrow.maxRecordsPerBatch
         */
        native void nativeSetBatchSize(int batch_size);

        /**
         * Set native env variables NATIVESQL_METRICS_TIME
         */
        native void nativeSetMetricsTime(boolean is_enable);

        /**
         * Closes the projector referenced by nativeHandler.
         *
         * @param nativeHandler nativeHandler that needs to be closed
         */
        native void nativeClose(long nativeHandler);
}
