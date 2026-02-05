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
val scalaVersion: String by project
val effectiveSparkFullVersion: String by rootProject.extra
val effectiveSparkPlainVersion: String by rootProject.extra
val effectiveHudiVersion: String? by rootProject.extra

val hudiVersion = effectiveHudiVersion ?: "1.1.0"

// Add Hudi-specific source directories
sourceSets {
    main {
        scala {
            srcDir("src-hudi/main/scala")
            srcDir("src-hudi/main/java")
            srcDir("src-hudi-spark${effectiveSparkPlainVersion}/main/scala")
            srcDir("src-hudi-spark${effectiveSparkPlainVersion}/main/java")
        }
        resources {
            srcDir("src-hudi/main/resources")
            srcDir("src-hudi-spark${effectiveSparkPlainVersion}/main/resources")
        }
    }
    test {
        scala {
            srcDir("src-hudi/test/scala")
            srcDir("src-hudi/test/java")
            srcDir("src-hudi-spark${effectiveSparkPlainVersion}/test/scala")
            srcDir("src-hudi-spark${effectiveSparkPlainVersion}/test/java")
        }
        resources {
            srcDir("src-hudi/test/resources")
            srcDir("src-hudi-spark${effectiveSparkPlainVersion}/test/resources")
        }
    }
}

dependencies {
    // Project dependencies
    implementation(project(":gluten-substrait"))

    // Hudi (provided)
    compileOnly("org.apache.hudi:hudi-spark${effectiveSparkPlainVersion.take(1)}.${effectiveSparkPlainVersion.drop(1)}-bundle_$scalaBinaryVersion:$hudiVersion")

    // Spark (provided)
    compileOnly("org.apache.spark:spark-sql_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-core_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-catalyst_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-hive_$scalaBinaryVersion:$effectiveSparkFullVersion")

    // Scala (provided)
    compileOnly("org.scala-lang:scala-library:$scalaVersion")

    // Test dependencies
    testImplementation("org.scalatest:scalatest_$scalaBinaryVersion:3.2.16")
    testImplementation("junit:junit:4.13.1")
}
