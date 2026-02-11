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
    id("gluten.scalatest")
    id("gluten.spotless")
}

val scalaBinaryVersion: String by project
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
    implementation(project(":gluten-substrait"))

    compileOnly(
        "org.apache.iceberg:iceberg-spark-runtime-${effectiveSparkPlainVersion.take(
            1,
        )}.${effectiveSparkPlainVersion.drop(1)}_$scalaBinaryVersion:$icebergVersion",
    )

    testImplementation(project(":backends-velox", "testArtifacts"))
    testImplementation(project(":gluten-substrait", "testArtifacts"))
    testImplementation(
        "org.apache.iceberg:iceberg-spark-runtime-${effectiveSparkPlainVersion.take(
            1,
        )}.${effectiveSparkPlainVersion.drop(1)}_$scalaBinaryVersion:$icebergVersion",
    )
}
