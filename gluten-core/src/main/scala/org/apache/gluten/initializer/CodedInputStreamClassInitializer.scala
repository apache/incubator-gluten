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
package org.apache.gluten.initializer

import org.apache.spark.internal.Logging

import java.lang.reflect.Field

/**
 * Pre-load the class instance for CodedInputStream and modify its defaultRecursionLimit to avoid
 * the limit is hit for deeply nested plans. This is based on the fact that the same class loader is
 * used to load this class in the program, then this modification will really take effect.
 */
object CodedInputStreamClassInitializer extends Logging {
  private val newDefaultRecursionLimit = 100000

  {
    try {
      // scalastyle:off classforname
      // Use the shaded class name.
      val clazz: Class[_] =
        Class.forName("org.apache.gluten.shaded.com.google.protobuf.CodedInputStream")
      // scalastyle:on classforname
      val field: Field = clazz.getDeclaredField("defaultRecursionLimit")
      field.setAccessible(true)
      // Enlarge defaultRecursionLimit whose original value is 100.
      field.setInt(null, newDefaultRecursionLimit)
      logInfo(
        s"The defaultRecursionLimit in protobuf has been increased to $newDefaultRecursionLimit")
    } catch {
      // The class is shaded during package phase (see package/pom.xml). We tolerate
      // this ClassNotFoundException in mvn test.
      case _: ClassNotFoundException =>
        logWarning("The defaultRecursionLimit in protobuf has not been modified.")
      case e: Exception => e.printStackTrace()
    }
  }
}
