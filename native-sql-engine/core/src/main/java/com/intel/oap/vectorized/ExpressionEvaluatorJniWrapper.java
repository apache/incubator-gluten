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
        public ExpressionEvaluatorJniWrapper(String tmp_dir, List<String> listJars)
                        throws IOException, IllegalAccessException, IllegalStateException {
                JniUtils jni = JniUtils.getInstance(tmp_dir);
                jni.setTempDir();
                jni.setJars(listJars);
                tmp_dir_path = jni.getTempDir();
        }

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
         *
         * @param batch_size numRows of one batch, use
         *                   spark.sql.execution.arrow.maxRecordsPerBatch
         */
        native void nativeSetMetricsTime(boolean is_enable);


        /**
         * Generates the projector module to evaluate the expressions with custom
         * configuration.
         *
         * @param memoryPool   The memoryPool ID of which the pool instance will be
         *                     used in expression evaluation
         * @param schemaBuf    The schema serialized as a protobuf. See Types.proto to
         *                     see the protobuf specification
         * @param exprListBuf  The serialized protobuf of the expression vector. Each
         *                     expression is created using TreeBuilder::MakeExpression.
         * @param resSchemaBuf The schema serialized as a protobuf. See Types.proto to
         *                     see the protobuf specification
         * @param finishReturn This parameter is used to indicate that this expression
         *                     should return when calling finish
         * @return A nativeHandler that is passed to the evaluateProjector() and
         *         closeProjector() methods
         */
        native long nativeBuild(long memoryPool, byte[] schemaBuf, byte[] exprListBuf, byte[] resSchemaBuf, boolean finishReturn)
                        throws RuntimeException, IOException;

        /**
         * Generates the projector module to evaluate the expressions with custom
         * configuration.
         *
         * @param memoryPool        The memoryPool ID of which the pool instance will be
         *                          used in expression evaluation
         * @param schemaBuf         The schema serialized as a protobuf. See Types.proto
         *                          to see the protobuf specification
         * @param exprListBuf       The serialized protobuf of the expression vector.
         *                          Each expression is created using
         *                          TreeBuilder::MakeExpression.
         * @param finishExprListBuf The serialized protobuf of the expression vector.
         *                          Each expression is created using
         *                          TreeBuilder::MakeExpression.
         * @return A nativeHandler that is passed to the evaluateProjector() and
         *         closeProjector() methods
         */
        native long nativeBuildWithFinish(long memoryPool, byte[] schemaBuf, byte[] exprListBuf, byte[] finishExprListBuf)
                        throws RuntimeException, IOException;

        /**
         * Set return schema for this expressionTree.
         *
         * @param nativeHandler nativeHandler representing expressions. Created using a
         *                      call to buildNativeCode
         * @param schemaBuf     The schema serialized as a protobuf. See Types.proto to
         *                      see the protobuf specification
         */
        native void nativeSetReturnFields(long nativeHandler, byte[] schemaBuf) throws RuntimeException;

        /**
         *
         * Spill data to disk.
         *
         * @param nativeHandler nativeHandler representing expressions. Created using a
         *                      call to buildNativeCode
         * @param size expected size to spill (in bytes)
         * @param callBySelf whether the caller is the expression evaluator itself, true
         *                   when running out of off-heap memory due to allocations from
         *                   the evaluator itself
         * @return actual spilled size
         */
        native long nativeSpill(long nativeHandler, long size, boolean callBySelf) throws RuntimeException;

        /**
         * Evaluate the expressions represented by the nativeHandler on a record batch
         * and store the output in ValueVectors. Throws an exception in case of errors
         *
         * @param nativeHandler nativeHandler representing expressions. Created using a
         *                      call to buildNativeCode
         * @param numRows       Number of rows in the record batch
         * @param bufAddrs      An array of memory addresses. Each memory address points
         *                      to a validity vector or a data vector (will add support
         *                      for offset vectors later).
         * @param bufSizes      An array of buffer sizes. For each memory address in
         *                      bufAddrs, the size of the buffer is present in bufSizes
         * @return A list of serialized record batch which can be used to build a List
         *         of ArrowRecordBatch
         */
        native byte[][] nativeEvaluate(long nativeHandler, int numRows, long[] bufAddrs,
                        long[] bufSizes) throws RuntimeException;

       native byte[][] nativeEvaluate2(long nativeHandler, byte[] bytes) throws RuntimeException;

        /**
         * Evaluate the expressions represented by the nativeHandler on a record batch
         * iterator. Throws an exception in case of errors
         *
         * @param nativeHandler a iterator instance carrying input record batches
         */
        native void nativeEvaluateWithIterator(long nativeHandler,
            ColumnarNativeIterator batchItr) throws RuntimeException;

        /**
         * Get native kernel signature by the nativeHandler.
         *
         * @param nativeHandler nativeHandler representing expressions. Created using a
         *                      call to buildNativeCode
         */
        native String nativeGetSignature(long nativeHandler) throws RuntimeException;

        /**
         * Evaluate the expressions represented by the nativeHandler on a record batch
         * and store the output in ValueVectors. Throws an exception in case of errors
         *
         * @param nativeHandler       nativeHandler representing expressions. Created
         *                            using a call to buildNativeCode
         * @param numRows             Number of rows in the record batch
         * @param bufAddrs            An array of memory addresses. Each memory address
         *                            points to a validity vector or a data vector (will
         *                            add support for offset vectors later).
         * @param bufSizes            An array of buffer sizes. For each memory address
         *                            in bufAddrs, the size of the buffer is present in
         *                            bufSizes
         * @param selectionVector     valid selected item record count
         * @param selectionVector     selectionVector memory address
         * @param selectionVectorSize selectionVector total size
         * @return A list of serialized record batch which can be used to build a List
         *         of ArrowRecordBatch
         */
        native byte[][] nativeEvaluateWithSelection(long nativeHandler, int numRows, long[] bufAddrs,
                        long[] bufSizes, int selectionVectorRecordCount, long selectionVectorAddr,
                        long selectionVectorSize) throws RuntimeException;

        native void nativeSetMember(long nativeHandler, int numRows, long[] bufAddrs, long[] bufSizes);

        /**
         * Evaluate the expressions represented by the nativeHandler on a record batch
         * and store the output in ValueVectors. Throws an exception in case of errors
         *
         * @param nativeHandler nativeHandler representing expressions. Created using a
         *                      call to buildNativeCode
         * @return A list of serialized record batch which can be used to build a List
         *         of ArrowRecordBatch
         */
        native byte[][] nativeFinish(long nativeHandler) throws RuntimeException;

        /**
         * Call Finish to get result, result will be as a iterator.
         *
         * @param nativeHandler nativeHandler of this expression
         * @return iterator instance id
         */
        native long nativeFinishByIterator(long nativeHandler) throws RuntimeException;

        /**
         * Call Finish to create a whole_stage_transfrom kernel, result will be as a iterator.
         *
         * @param nativeHandler nativeHandler of this expression
         * @return iterator instance id
         */
        native long nativeCreateKernelWithIterator(long nativeHandler, byte[] wsInSchemaBuf,
                                                   byte[] wsExprListBuf, byte[] wsResSchemaBuf,
                                                   byte[] inExprListBuf,
                                                   ColumnarNativeIterator batchItr,
                                                   long[] dependencies, boolean finishReturn) throws RuntimeException;

        /**
         * Set another evaluator's iterator as this one's dependency.
         *
         * @param nativeHandler   nativeHandler of this expression
         * @param childInstanceId childInstanceId of a child BatchIterator
         * @param index           exptected index of the output of BatchIterator
         */
        native void nativeSetDependency(long nativeHandler, long childInstanceId, int index) throws RuntimeException;

        /**
         * Closes the projector referenced by nativeHandler.
         *
         * @param nativeHandler nativeHandler that needs to be closed
         */
        native void nativeClose(long nativeHandler);
}
