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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

/**
 * Convention plugin for shading dependencies.
 * Configures the Shadow plugin with Gluten-specific relocations.
 */

plugins {
    id("com.gradleup.shadow")
}

val glutenShadePackageName: String by project

tasks.withType<ShadowJar>().configureEach {
    // Relocate protobuf
    relocate("com.google.protobuf", "$glutenShadePackageName.com.google.protobuf")

    // Relocate guava (excluding jimfs)
    relocate("com.google.common", "$glutenShadePackageName.com.google.common") {
        exclude("com.google.common.jimfs.**")
    }

    // Relocate other Google libraries
    relocate("com.google.thirdparty", "$glutenShadePackageName.com.google.thirdparty")
    relocate("com.google.errorprone.annotations", "$glutenShadePackageName.com.google.errorprone.annotations")
    relocate("com.google.j2objc", "$glutenShadePackageName.com.google.j2objc")
    relocate("com.google.gson", "$glutenShadePackageName.com.google.gson")

    // Relocate Arrow (excluding C bindings and dataset)
    relocate("org.apache.arrow", "$glutenShadePackageName.org.apache.arrow") {
        exclude("org.apache.arrow.c.*")
        exclude("org.apache.arrow.c.jni.*")
        exclude("org.apache.arrow.dataset.**")
    }

    // Relocate FlatBuffers
    relocate("com.google.flatbuffers", "$glutenShadePackageName.com.google.flatbuffers")

    // Exclude signatures
    exclude("META-INF/*.SF")
    exclude("META-INF/*.DSA")
    exclude("META-INF/*.RSA")
    exclude("META-INF/DEPENDENCIES")
    exclude("META-INF/LICENSE.txt")
    exclude("META-INF/NOTICE.txt")
    exclude("LICENSE.txt")
    exclude("NOTICE.txt")

    // Merge service files
    mergeServiceFiles()

    // Archive settings
    archiveClassifier.set("")
}
