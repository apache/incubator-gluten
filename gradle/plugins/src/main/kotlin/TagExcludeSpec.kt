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

/**
 * Gradle [Spec] that excludes test classes based on ScalaTest tag annotations.
 *
 * The ScalaTest JUnit 5 engine does not support JUnit Platform's includeTags/excludeTags
 * filtering. This spec implements tag filtering at the Gradle level by scanning the class
 * file constant pool for annotation type descriptors (e.g., `Lorg/apache/gluten/tags/UDFTest;`).
 *
 * @param tagsToExclude fully qualified annotation class names to exclude
 *        (e.g., `org.apache.gluten.tags.UDFTest`)
 * @param tagsToInclude if non-empty, only classes with at least one of these tags will run;
 *        classes without any of these tags are excluded
 */
class TagExcludeSpec(
    private val tagsToExclude: Set<String>,
    private val tagsToInclude: Set<String>
) : Spec<FileTreeElement> {

    // Convert class names to JVM type descriptors for constant pool matching.
    // e.g., "org.apache.gluten.tags.UDFTest" -> "Lorg/apache/gluten/tags/UDFTest;"
    private val excludeDescriptors = tagsToExclude.map { "L${it.replace('.', '/')};" }.toSet()
    private val includeDescriptors = tagsToInclude.map { "L${it.replace('.', '/')};" }.toSet()

    override fun isSatisfiedBy(element: FileTreeElement): Boolean {
        val f = element.file
        if (!f.name.endsWith(".class") || !f.exists()) return false
        if (excludeDescriptors.isEmpty() && includeDescriptors.isEmpty()) return false
        return try {
            val utf8Entries = readConstantPoolUtf8Entries(f)
            // Exclude if any excluded tag is found
            if (excludeDescriptors.any { it in utf8Entries }) return true
            // Exclude if include filter is active and no included tag is found
            if (includeDescriptors.isNotEmpty() && includeDescriptors.none { it in utf8Entries }) return true
            false
        } catch (_: Exception) {
            false
        }
    }

    private fun readConstantPoolUtf8Entries(f: java.io.File): Set<String> {
        val entries = mutableSetOf<String>()
        DataInputStream(f.inputStream()).use { din ->
            din.skipBytes(8) // magic(4) + version(4)
            val cpCount = din.readUnsignedShort()
            var i = 1
            while (i < cpCount) {
                when (din.readUnsignedByte()) {
                    1 -> { // Utf8
                        val len = din.readUnsignedShort()
                        val bytes = ByteArray(len)
                        din.readFully(bytes)
                        entries.add(String(bytes))
                    }
                    3, 4 -> din.skipBytes(4)    // Integer, Float
                    5, 6 -> { din.skipBytes(8); i++ } // Long, Double (2 pool slots)
                    7, 8, 16, 19, 20 -> din.skipBytes(2)
                    9, 10, 11, 12, 17, 18 -> din.skipBytes(4)
                    15 -> din.skipBytes(3)      // MethodHandle
                }
                i++
            }
        }
        return entries
    }
}
