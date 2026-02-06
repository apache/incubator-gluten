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
val effectivePaimonVersion: String? by rootProject.extra

val paimonVersion = effectivePaimonVersion ?: "1.3.0"

// Add Paimon-specific source directories
sourceSets {
    main {
        scala {
            srcDir("src-paimon/main/scala")
            srcDir("src-paimon/main/java")
        }
        resources {
            srcDir("src-paimon/main/resources")
        }
    }
    test {
        scala {
            srcDir("src-paimon/test/scala")
            srcDir("src-paimon/test/java")
        }
        resources {
            srcDir("src-paimon/test/resources")
        }
    }
}

dependencies {
    // Project dependencies
    implementation(project(":gluten-substrait"))

    // Paimon (provided)
    compileOnly("org.apache.paimon:paimon-spark_$scalaBinaryVersion:$paimonVersion")

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

    // Paimon for tests
    testImplementation("org.apache.paimon:paimon-spark_$scalaBinaryVersion:$paimonVersion")

    // Spark test JARs
    testImplementation("org.apache.spark:spark-core_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
    testImplementation("org.apache.spark:spark-sql_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
    testImplementation("org.apache.spark:spark-catalyst_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
    testImplementation("org.apache.spark:spark-hive_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
    if (effectiveSparkFullVersion.startsWith("4")) {
        testImplementation("org.apache.spark:spark-common-utils_$scalaBinaryVersion:$effectiveSparkFullVersion")
    }
}
