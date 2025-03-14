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

  def modifyDefaultRecursionLimitUnsafe: Unit = {
    try {
      // scalastyle:off classforname
      val clazz: Class[_] =
        try {
          // Use the shaded class name.
          Class.forName("org.apache.gluten.shaded.com.google.protobuf.CodedInputStream")
        } catch {
          // The above class is shaded in final package phase (see package/pom.xml).
          // If ClassNotFoundException happens, e.g., in mvn test, load the original class instead.
          case _: ClassNotFoundException =>
            Class.forName("com.google.protobuf.CodedInputStream")
        }
      // scalastyle:on classforname
      val field: Field = clazz.getDeclaredField("defaultRecursionLimit")
      field.setAccessible(true)
      // Enlarge defaultRecursionLimit whose original value is 100.
      field.setInt(null, newDefaultRecursionLimit)
      logInfo(
        s"The defaultRecursionLimit in protobuf has been increased to $newDefaultRecursionLimit")
    } catch {
      case e: Exception =>
        log.error("Failed to modify the DefaultRecursionLimit in protobuf: " + e.getMessage)
    }
  }
}
