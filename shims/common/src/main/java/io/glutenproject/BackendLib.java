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
package io.glutenproject;

// FIXME The usage of this class should be reconsidered. Backend-specific codes should be avoid
// in common module.
// same as GlutenConfig GLUTEN_VELOX_BACKEND...
public class BackendLib {
  private static Name LOADED_BACKEND_NAME = Name.UNKNOWN;

  private BackendLib() {
  }

  public static synchronized void setLoadedBackendName(Name name) {
    LOADED_BACKEND_NAME = name;
  }

  public static synchronized Name getLoadedBackendName() {
    return LOADED_BACKEND_NAME;
  }

  public enum Name {
    VELOX,
    CH,
    GAZELLE_CPP,
    UNKNOWN
  }
}
