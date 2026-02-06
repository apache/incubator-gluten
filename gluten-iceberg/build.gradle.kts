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
val effectiveIcebergVersion: String? by rootProject.extra
val effectiveIcebergBinaryVersion: String? by rootProject.extra

val icebergVersion = effectiveIcebergVersion ?: "1.10.0"
val icebergBinaryVersion = effectiveIcebergBinaryVersion ?: "10"

// Add Iceberg-specific source directories
sourceSets {
    main {
        scala {
            srcDir("src-iceberg/main/scala")
            srcDir("src-iceberg/main/java")
            srcDir("src-iceberg$icebergBinaryVersion/main/scala")
            srcDir("src-iceberg$icebergBinaryVersion/main/java")
            srcDir("src-iceberg-spark$effectiveSparkPlainVersion/main/scala")
            srcDir("src-iceberg-spark$effectiveSparkPlainVersion/main/java")
        }
        resources {
            srcDir("src-iceberg/main/resources")
        }
    }
    test {
        scala {
            srcDir("src-iceberg/test/scala")
            srcDir("src-iceberg/test/java")
            srcDir("src-iceberg$icebergBinaryVersion/test/scala")
            srcDir("src-iceberg$icebergBinaryVersion/test/java")
        }
        resources {
            srcDir("src-iceberg/test/resources")
            srcDir("src-iceberg$icebergBinaryVersion/test/resources")
        }
    }
}

dependencies {
    // Project dependencies
    implementation(project(":gluten-substrait"))

    // Iceberg (provided)
    compileOnly(
        "org.apache.iceberg:iceberg-spark-runtime-${effectiveSparkPlainVersion.take(
            1,
        )}.${effectiveSparkPlainVersion.drop(1)}_$scalaBinaryVersion:$icebergVersion",
    )

    // Spark (provided)
    compileOnly("org.apache.spark:spark-sql_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-core_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-catalyst_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-hive_$scalaBinaryVersion:$effectiveSparkFullVersion")

    // Test dependencies
    testImplementation("org.scalatest:scalatest_$scalaBinaryVersion:3.2.16")
    testImplementation("junit:junit:4.13.1")

    // Test JARs from other modules (WholeStageTransformerSuite etc.)
    testImplementation(project(":backends-velox", "testArtifacts"))
    testImplementation(project(":gluten-substrait", "testArtifacts"))

    // Iceberg for tests
    testImplementation(
        "org.apache.iceberg:iceberg-spark-runtime-${effectiveSparkPlainVersion.take(
            1,
        )}.${effectiveSparkPlainVersion.drop(1)}_$scalaBinaryVersion:$icebergVersion",
    )

    // Spark test JARs
    testImplementation("org.apache.spark:spark-core_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
    testImplementation("org.apache.spark:spark-sql_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
    testImplementation("org.apache.spark:spark-catalyst_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
    testImplementation("org.apache.spark:spark-hive_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
    if (effectiveSparkFullVersion.startsWith("4")) {
        testImplementation("org.apache.spark:spark-common-utils_$scalaBinaryVersion:$effectiveSparkFullVersion")
    }
}
