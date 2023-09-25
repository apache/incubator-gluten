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
package io.glutenproject.spark.sql.execution.datasources.velox;

import io.glutenproject.init.JniInitialized;
import io.glutenproject.init.JniUtils;

import org.apache.spark.sql.execution.datasources.VeloxColumnarBatchIterator;

import java.io.IOException;
import java.util.Map;

/** The jni file is at `cpp/core/jni/JniWrapper.cc` */
public class DatasourceJniWrapper extends JniInitialized {

  public DatasourceJniWrapper() throws IOException {}

  public long nativeInitDatasource(
      String filePath,
      long cSchema,
      long executionCtxHandle,
      long memoryManagerHandle,
      Map<String, String> options) {
    return nativeInitDatasource(
        filePath, cSchema, executionCtxHandle, memoryManagerHandle, JniUtils.toNativeConf(options));
  }

  public native long nativeInitDatasource(
      String filePath,
      long cSchema,
      long executionCtxHandle,
      long memoryManagerHandle,
      byte[] options);

  public native void inspectSchema(long executionCtxHandle, long dsHandle, long cSchemaAddress);

  public native void close(long executionCtxHandle, long dsHandle);

  public native void write(
      long executionCtxHandle, long dsHandle, VeloxColumnarBatchIterator iterator);
}
