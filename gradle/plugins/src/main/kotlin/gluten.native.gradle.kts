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

/**
 * Convention plugin for native C++ build integration.
 * Provides CMake build tasks for the Velox backend.
 */

plugins {
    base
}

val cppBuildDir = project.file("../cpp/build")
val cppReleasesDir = file("$cppBuildDir/releases")

// Task to build native libraries using CMake
val buildNative by tasks.registering(Exec::class) {
    group = "build"
    description = "Build native C++ libraries using CMake"

    workingDir = project.file("../cpp")

    commandLine(
        "cmake",
        "--build", "build",
        "--parallel", Runtime.getRuntime().availableProcessors().toString()
    )

    outputs.dir(cppReleasesDir)
}

// Task to configure CMake
val configureNative by tasks.registering(Exec::class) {
    group = "build"
    description = "Configure native C++ build using CMake"

    workingDir = project.file("../cpp")

    doFirst {
        project.file("../cpp/build").mkdirs()
    }

    commandLine(
        "cmake",
        "-B", "build",
        "-DCMAKE_BUILD_TYPE=Release"
    )
}

// Task to clean native build
val cleanNative by tasks.registering(Delete::class) {
    group = "build"
    description = "Clean native C++ build artifacts"

    delete(cppBuildDir)
}

// Make native resources available to the JAR
tasks.withType<ProcessResources>().configureEach {
    dependsOn(buildNative)

    val platform = rootProject.extra.get("platform") as String
    val arch = rootProject.extra.get("arch") as String

    from(cppReleasesDir) {
        into("$platform/$arch")
    }
}
