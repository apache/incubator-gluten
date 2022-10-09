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

package io.glutenproject.test;

public class TestStats {
  // use the gluten backend to execute the query
  public static boolean offloadGluten = false;
  public static int suiteTestNumber = 0;
  public static int offloadGlutenTestNumber = 0;

  public static void reset() {
    offloadGluten = false;
    suiteTestNumber = 0;
    offloadGlutenTestNumber = 0;
  }
}
