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
package io.glutenproject.memory.alloc;

import org.apache.spark.util.TaskResource;

public class NativeMemoryManager implements TaskResource {

  private final long nativeInstanceId;
  private final String name;

  private NativeMemoryManager(String name, long nativeInstanceId) {
    this.name = name;
    this.nativeInstanceId = nativeInstanceId;
  }

  public static NativeMemoryManager create(String name, ReservationListener listener) {
    long allocatorId = NativeMemoryAllocators.getDefault().globalInstance().getNativeInstanceId();
    return new NativeMemoryManager(name, createListenableManager(name, allocatorId, listener));
  }

  public long getNativeInstanceId() {
    return this.nativeInstanceId;
  }

  public void close() {
    releaseManager(this.nativeInstanceId);
  }

  public static native long createListenableManager(
      String name, long allocatorId, ReservationListener listener);

  private static native void releaseManager(long allocatorId);

  @Override
  public void release() throws Exception {
    releaseManager(nativeInstanceId);
  }

  @Override
  public long priority() {
    return 0L;
  }

  @Override
  public String resourceName() {
    return name + "_mem";
  }
}
