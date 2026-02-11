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

import org.gradle.api.file.FileTreeElement
import org.gradle.api.specs.Spec
import java.io.DataInputStream
import java.lang.reflect.Modifier

/**
 * Gradle [Spec] that excludes abstract classes and interfaces from test scanning.
 *
 * The ScalaTest JUnit 5 engine fails with [InstantiationException] when Gradle sends
 * abstract test base classes or Scala traits to it. This spec reads the `access_flags`
 * from the class file header to detect and exclude them before they reach the engine.
 */
class AbstractClassExcludeSpec : Spec<FileTreeElement> {
    override fun isSatisfiedBy(element: FileTreeElement): Boolean {
        val f = element.file
        if (!f.name.endsWith(".class") || !f.exists()) return false
        return try {
            DataInputStream(f.inputStream()).use { din ->
                din.skipBytes(8) // magic(4) + version(4)
                // Skip constant pool
                val cpCount = din.readUnsignedShort()
                var i = 1
                while (i < cpCount) {
                    when (din.readUnsignedByte()) {
                        1 -> din.skipBytes(din.readUnsignedShort()) // Utf8
                        3, 4 -> din.skipBytes(4)    // Integer, Float
                        5, 6 -> { din.skipBytes(8); i++ } // Long, Double (2 pool slots)
                        7, 8, 16, 19, 20 -> din.skipBytes(2)
                        9, 10, 11, 12, 17, 18 -> din.skipBytes(4)
                        15 -> din.skipBytes(3)      // MethodHandle
                    }
                    i++
                }
                val accessFlags = din.readUnsignedShort()
                Modifier.isAbstract(accessFlags) || Modifier.isInterface(accessFlags)
            }
        } catch (_: Exception) {
            false
        }
    }
}
