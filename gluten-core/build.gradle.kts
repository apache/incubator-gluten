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

import java.time.LocalDateTime

plugins {
    id("gluten.scala-library")
    id("gluten.protobuf")
    id("gluten.spotless")
}

val scalaBinaryVersion: String by project
val scalaVersion: String by project
val protobufVersion: String by project
val effectiveSparkFullVersion: String by rootProject.extra
val effectiveHadoopVersion: String by rootProject.extra

val backend: String by project

dependencies {
    // Project dependencies - use api to expose transitive dependencies
    api(project(":gluten-ras-common"))

    // Spark (provided)
    compileOnly("org.apache.spark:spark-sql_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-core_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-catalyst_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-hive_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-kvstore_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-network-common_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-network-shuffle_$scalaBinaryVersion:$effectiveSparkFullVersion")

    // Hadoop (provided)
    compileOnly("org.apache.hadoop:hadoop-client:$effectiveHadoopVersion")

    // Protobuf (provided - will be shaded in final JAR)
    compileOnly("com.google.protobuf:protobuf-java:$protobufVersion")

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
}

sourceSets {
    main {
        proto {
            srcDir("src/main/resources/org/apache/gluten/proto")
        }
    }
}

// Generate build info
val generateBuildInfo by tasks.registering {
    group = "build"
    description = "Generate Gluten build info properties file"

    val outputDir = layout.buildDirectory.dir("generated-resources")
    outputs.dir(outputDir)

    doLast {
        val propsFile = outputDir.get().file("gluten-build-info.properties").asFile
        propsFile.parentFile.mkdirs()

        fun gitOutput(vararg args: String): String =
            try {
                ProcessBuilder(*args)
                    .directory(projectDir)
                    .redirectErrorStream(true)
                    .start()
                    .inputStream.bufferedReader().readText().trim()
            } catch (e: Exception) {
                "unknown"
            }

        propsFile.writeText(
            """
            |gluten_version=${project.version}
            |backend_type=$backend
            |branch=${gitOutput("git", "rev-parse", "--abbrev-ref", "HEAD")}
            |revision=${gitOutput("git", "rev-parse", "HEAD")}
            |revision_time=${gitOutput("git", "show", "-s", "--format=%ci", "HEAD")}
            |url=${gitOutput("git", "config", "--get", "remote.origin.url")}
            |java_version=${System.getProperty("java.version")}
            |scala_version=$scalaVersion
            |spark_version=$effectiveSparkFullVersion
            |hadoop_version=$effectiveHadoopVersion
            |build_user=${System.getProperty("user.name")}
            |date=${LocalDateTime.now()}
            """.trimMargin(),
        )
    }
}

tasks.processResources {
    dependsOn(generateBuildInfo)
    from(layout.buildDirectory.dir("generated-resources"))
}
