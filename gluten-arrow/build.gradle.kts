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
    id("gluten.scala-library")
    id("gluten.spotless")
}

val scalaBinaryVersion: String by project
val effectiveSparkFullVersion: String by rootProject.extra
val effectiveSparkPlainVersion: String by rootProject.extra
val effectiveArrowVersion: String by rootProject.extra

// Allow switching between arrow-memory-unsafe (default) and arrow-memory-netty
// via -ParrowMemoryArtifact=arrow-memory-netty
val arrowMemoryArtifact = providers.gradleProperty("arrowMemoryArtifact").getOrElse("arrow-memory-unsafe")

dependencies {
    api(project(":gluten-substrait"))
    implementation(project(":shims-common"))
    implementation(project(":shims-spark$effectiveSparkPlainVersion"))

    api("org.apache.arrow:arrow-vector:$effectiveArrowVersion")
    api("org.apache.arrow:arrow-memory-core:$effectiveArrowVersion")
    implementation("org.apache.arrow:$arrowMemoryArtifact:$effectiveArrowVersion")
    api("org.apache.arrow:arrow-c-data:$effectiveArrowVersion")
    api("org.apache.arrow:arrow-dataset:$effectiveArrowVersion")

    compileOnly("org.apache.spark:spark-network-common_$scalaBinaryVersion:$effectiveSparkFullVersion")

    testImplementation(project(":gluten-core", "testArtifacts"))
}
