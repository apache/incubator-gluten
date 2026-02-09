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

plugins {
    `kotlin-dsl`
}

repositories {
    gradlePluginPortal()
    mavenCentral()
}

// spotless-plugin-gradle 6.25.0 publishes variants declaring Java 11 compatibility.
// On JDK 8, Gradle's variant-aware resolution rejects it. Relax the Java version
// attribute on resolvable configurations only (not consumable ones, which would
// propagate the Java 11 requirement to consumers of this project).
configurations.matching { it.isCanBeResolved && !it.isCanBeConsumed }.configureEach {
    attributes {
        attribute(
            TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE,
            JavaVersion.current().majorVersion.toInt().coerceAtLeast(11)
        )
    }
}

dependencies {
    implementation("com.diffplug.spotless:spotless-plugin-gradle:6.25.0")
    implementation("com.gradleup.shadow:shadow-gradle-plugin:8.3.0")
    implementation("com.google.protobuf:protobuf-gradle-plugin:0.9.4")
    implementation("com.github.maiflai:gradle-scalatest:0.32")
}
