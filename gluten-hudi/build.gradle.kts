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
val sparkVersion: String by project
val effectiveSparkPlainVersion: String by rootProject.extra
val effectiveHudiVersion: String? by rootProject.extra

val hudiVersion = effectiveHudiVersion ?: "0.15.0"

// Add Hudi-specific source directories
sourceSets {
    main {
        scala {
            srcDir("src-hudi/main/scala")
            srcDir("src-hudi/main/java")
            srcDir("src-hudi-spark$effectiveSparkPlainVersion/main/scala")
            srcDir("src-hudi-spark$effectiveSparkPlainVersion/main/java")
        }
        resources {
            srcDir("src-hudi/main/resources")
            srcDir("src-hudi-spark$effectiveSparkPlainVersion/main/resources")
        }
    }
    test {
        scala {
            srcDir("src-hudi/test/scala")
            srcDir("src-hudi/test/java")
            srcDir("src-hudi-spark$effectiveSparkPlainVersion/test/scala")
            srcDir("src-hudi-spark$effectiveSparkPlainVersion/test/java")
        }
        resources {
            srcDir("src-hudi/test/resources")
            srcDir("src-hudi-spark$effectiveSparkPlainVersion/test/resources")
        }
    }
}

dependencies {
    implementation(project(":gluten-substrait"))

    compileOnly(
        "org.apache.hudi:hudi-spark$sparkVersion-bundle_$scalaBinaryVersion:$hudiVersion",
    )

    testImplementation(project(":backends-velox", "testArtifacts"))
    testImplementation(project(":gluten-substrait", "testArtifacts"))
    testImplementation(
        "org.apache.hudi:hudi-spark$sparkVersion-bundle_$scalaBinaryVersion:$hudiVersion",
    )
}
