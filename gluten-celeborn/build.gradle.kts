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
val celebornVersion: String by project

// Add Celeborn-specific source directories
sourceSets {
    main {
        scala {
            srcDir("src-celeborn/main/scala")
            srcDir("src-celeborn/main/java")
            srcDir("src-celeborn-spark${effectiveSparkPlainVersion}/main/scala")
            srcDir("src-celeborn-spark${effectiveSparkPlainVersion}/main/java")
        }
        resources {
            srcDir("src-celeborn/main/resources")
            srcDir("src-celeborn-spark${effectiveSparkPlainVersion}/main/resources")
        }
    }
    test {
        scala {
            srcDir("src-celeborn/test/scala")
            srcDir("src-celeborn/test/java")
            srcDir("src-celeborn-spark${effectiveSparkPlainVersion}/test/scala")
            srcDir("src-celeborn-spark${effectiveSparkPlainVersion}/test/java")
        }
        resources {
            srcDir("src-celeborn/test/resources")
            srcDir("src-celeborn-spark${effectiveSparkPlainVersion}/test/resources")
        }
    }
}

dependencies {
    // Project dependencies
    implementation(project(":gluten-substrait"))

    // Celeborn (provided)
    compileOnly("org.apache.celeborn:celeborn-client-spark-${effectiveSparkPlainVersion.take(1)}-shaded_$scalaBinaryVersion:$celebornVersion")

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
