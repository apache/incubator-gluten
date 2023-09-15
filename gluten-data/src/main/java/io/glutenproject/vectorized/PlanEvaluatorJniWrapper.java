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
package io.glutenproject.vectorized;

import io.glutenproject.init.JniInitialized;
import io.glutenproject.validate.NativePlanValidationInfo;

/**
 * This class is implemented in JNI. This provides the Java interface to invoke functions in JNI.
 * This file is used to generate the .h files required for jni. Avoid all external dependencies in
 * this file.
 */
public class PlanEvaluatorJniWrapper extends JniInitialized {

  /** Wrapper for native API. */
  public PlanEvaluatorJniWrapper() {}

  /**
   * Validate the Substrait plan in native compute engine.
   *
   * @param subPlan the Substrait plan in binary format.
   * @return whether the computing of this plan is supported in native and related info.
   */
  native NativePlanValidationInfo nativeValidateWithFailureReason(byte[] subPlan);

  /**
   * Create a native compute kernel and return a columnar result iterator.
   *
   * @param memoryManagerHandle NativeMemoryManager instance handle
   * @return iterator instance id
   */
  public native long nativeCreateKernelWithIterator(
      long ctxHandle,
      long memoryManagerHandle,
      byte[] wsPlan,
      GeneralInIterator[] batchItr,
      int stageId,
      int partitionId,
      long taskId,
      boolean saveInputToFile,
      String spillDir,
      byte[] confPlan)
      throws RuntimeException;

  /** Create a native compute kernel and return a row iterator. */
  native long nativeCreateKernelWithRowIterator(byte[] wsPlan) throws RuntimeException;

  /**
   * Closes the projector referenced by nativeHandler.
   *
   * @param nativeHandler nativeHandler that needs to be closed
   */
  native void nativeClose(long nativeHandler);
}
