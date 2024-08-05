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

package org.apache.gluten.integration;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class SparkJvmOptions {
  private static final String MODULE_OPTIONS_CLASS_NAME = "org.apache.spark.launcher.JavaModuleOptions";

  public static String read() {
    try {
      final Class<?> clazz = Class.forName("org.apache.spark.launcher.JavaModuleOptions");
      final Method method = clazz.getMethod("defaultModuleOptions");
      return (String) method.invoke(null);
    } catch (ClassNotFoundException e) {
      // Could happen in Spark 3.2 which doesn't have this class yet.
      return "";
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) {
    System.out.println(read());
  }
}
