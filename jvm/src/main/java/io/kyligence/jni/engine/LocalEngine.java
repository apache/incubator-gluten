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

package io.kyligence.jni.engine;

import java.io.Closeable;
import java.io.IOException;

import com.intel.oap.row.SparkRowInfo;

public class LocalEngine implements Closeable {
    public static native long test(int a, int b);

    public static native void initEngineEnv();

    private long nativeExecutor;
    private byte[] plan;

    public LocalEngine(byte[] plan) {
        this.plan = plan;
    }

    public native void execute();

    public native boolean hasNext();

    public native SparkRowInfo next();


    @Override
    public native void close() throws IOException;
}
