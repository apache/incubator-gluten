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
    id("gluten.protobuf")
    id("gluten.spotless")
}

val scalaBinaryVersion: String by project
val protobufVersion: String by project
val effectiveSparkFullVersion: String by rootProject.extra
val effectiveHadoopVersion: String by rootProject.extra

// C++ build directories for ClickHouse
val cppChBuildDir = file("../cpp-ch/build")
val cppChReleasesDir = file("$cppChBuildDir/releases")

dependencies {
    // Project dependencies
    implementation(project(":gluten-substrait"))

    // Protobuf
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")

    // Spark (provided)
    compileOnly("org.apache.spark:spark-sql_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-core_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-catalyst_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-hive_$scalaBinaryVersion:$effectiveSparkFullVersion")

    // Hadoop (provided)
    compileOnly("org.apache.hadoop:hadoop-client:$effectiveHadoopVersion")

    implementation("org.scala-lang.modules:scala-collection-compat_$scalaBinaryVersion:2.11.0")

    // Jackson (provided)
    compileOnly("com.fasterxml.jackson.core:jackson-databind:2.18.2")
    compileOnly("com.fasterxml.jackson.core:jackson-annotations:2.18.2")
    compileOnly("com.fasterxml.jackson.core:jackson-core:2.18.2")
    compileOnly("com.fasterxml.jackson.module:jackson-module-scala_$scalaBinaryVersion:2.18.2")

    // Test dependencies
    testImplementation("org.scalatest:scalatest_$scalaBinaryVersion:3.2.16")
    testImplementation("org.scalacheck:scalacheck_$scalaBinaryVersion:1.17.0")
    testImplementation("org.mockito:mockito-core:2.23.4")
    testImplementation("junit:junit:4.13.1")
    testImplementation("org.scalatestplus:scalatestplus-mockito_$scalaBinaryVersion:1.0.0-M2")
    testImplementation("org.scalatestplus:scalatestplus-scalacheck_$scalaBinaryVersion:3.1.0.0-RC2")

    // Spark test JARs
    testImplementation("org.apache.spark:spark-core_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
    testImplementation("org.apache.spark:spark-sql_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
    testImplementation("org.apache.spark:spark-catalyst_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
    testImplementation("org.apache.spark:spark-hive_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
}

sourceSets {
    main {
        proto {
            srcDir("src/main/resources/org/apache/gluten/proto")
        }
    }
}

// Include native libraries in the JAR
val platform = rootProject.extra.get("platform") as String
val arch = rootProject.extra.get("arch") as String

tasks.processResources {
    from(cppChReleasesDir) {
        into("$platform/$arch")
    }
}
