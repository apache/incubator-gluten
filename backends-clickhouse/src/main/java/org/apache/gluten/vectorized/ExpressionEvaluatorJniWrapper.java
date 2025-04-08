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
package org.apache.gluten.vectorized;

import org.apache.gluten.execution.ColumnarNativeIterator;

/**
 * This class is implemented in JNI. This provides the Java interface to invoke functions in JNI.
 * This file is used to generate the .h files required for jni. Avoid all external dependencies in
 * this file.
 */
public class ExpressionEvaluatorJniWrapper {

  /** Call initNative to initialize native computing. */
  static native void nativeInitNative(byte[] confAsPlan);

  /** Call finalizeNative to finalize native computing for each SparkSession. */
  static native void nativeFinalizeNative();

  /** Call finalizeNative to finalize native computing when jvm shutdown. */
  static native void nativeDestroyNative();

  /**
   * Create a native compute kernel and return a columnar result iterator.
   *
   * @return iterator instance id
   */
  public static native long nativeCreateKernelWithIterator(
      byte[] wsPlan,
      byte[][] splitInfo,
      ColumnarNativeIterator[] batchItr,
      byte[] confArray,
      boolean materializeInput,
      int partitionIndex);

  public static native void updateQueryRuntimeSettings(byte[] settings);
}
