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
package org.apache.spark.util.sketch;

import org.apache.gluten.exec.Runtime;
import org.apache.gluten.exec.RuntimeAware;
import org.apache.gluten.exec.Runtimes;

public class VeloxBloomFilterJniWrapper implements RuntimeAware {
  private final Runtime runtime;

  private VeloxBloomFilterJniWrapper(Runtime runtime) {
    this.runtime = runtime;
  }

  public static VeloxBloomFilterJniWrapper create() {
    return new VeloxBloomFilterJniWrapper(Runtimes.contextInstance());
  }

  @Override
  public long handle() {
    return runtime.getHandle();
  }

  public native long empty(int capacity);

  public native long init(byte[] data);

  public native void insertLong(long handle, long item);

  public native boolean mightContainLong(long handle, long item);

  public native void mergeFrom(long handle, long other);

  public native byte[] serialize(long handle);
}
