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
 * Convention plugin for Java compilation settings.
 * Configures Java source/target compatibility and compiler options.
 */

plugins {
    java
}

val javaVersion: String by project

val javaVersionInt = javaVersion.toIntOrNull() ?: 17

java {
    sourceCompatibility = JavaVersion.toVersion(javaVersionInt)
    targetCompatibility = JavaVersion.toVersion(javaVersionInt)

    withSourcesJar()
}

// When the running JDK is newer than targetCompatibility (e.g. JDK 21 with target 17),
// Gradle may publish outgoing variants declaring the JDK version instead of the target.
// Explicitly set TargetJvmVersion on all configurations so inter-project dependencies
// resolve correctly regardless of the build JDK.
configurations.matching { it.isCanBeConsumed && !it.isCanBeResolved }.configureEach {
    attributes {
        attribute(
            TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE,
            javaVersionInt
        )
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.compilerArgs.addAll(listOf("-Xlint:all,-serial,-path"))
    options.isFork = true
    options.forkOptions.jvmArgs = (options.forkOptions.jvmArgs ?: mutableListOf()).apply {
        add("-Dfile.encoding=UTF-8")
    }
}

tasks.withType<Javadoc>().configureEach {
    options.encoding = "UTF-8"
    isFailOnError = false
}

// Make compileOnly dependencies available for test compilation
// This is needed because Spark dependencies are declared as compileOnly
// but are required for test compilation
configurations {
    testImplementation {
        extendsFrom(configurations.compileOnly.get())
    }
}
