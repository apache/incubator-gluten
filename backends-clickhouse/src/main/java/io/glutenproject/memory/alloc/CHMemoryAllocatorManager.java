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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Vector;

public class CHMemoryAllocatorManager implements NativeMemoryAllocatorManager {

  private static Logger LOGGER = LoggerFactory.getLogger(CHMemoryAllocatorManager.class);

  private static final List<NativeMemoryAllocator> LEAKED = new Vector<>();
  private final NativeMemoryAllocator managed;

  public CHMemoryAllocatorManager(NativeMemoryAllocator managed) {
    this.managed = managed;
  }

  @Override
  public void release() throws Exception {
    managed.close();
    managed.listener().inactivate();
  }

  @Override
  public NativeMemoryAllocator getManaged() {
    return managed;
  }

  @Override
  public long priority() {
    return 0L; // lowest priority
  }
}
