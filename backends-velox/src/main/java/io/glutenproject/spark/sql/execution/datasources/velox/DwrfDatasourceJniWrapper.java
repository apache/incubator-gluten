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

import java.io.IOException;

import io.glutenproject.vectorized.JniLibLoader;
import io.glutenproject.vectorized.JniWorkspace;

public class DwrfDatasourceJniWrapper {

    public DwrfDatasourceJniWrapper() throws IOException {
        final JniLibLoader loader = JniWorkspace.getDefault().libLoader();
        loader.loadEssentials();
        loader.mapAndLoad("velox");
    }
    public native long nativeInitDwrfDatasource(String filePath, byte[] schema);
    public native byte[] inspectSchema(long instanceId);
    public native void close(long instanceId);

    public native void write(long instanceId, byte[] schema, int numRows, long[] bufAddrs,
                             long[] bufSizes);
}
