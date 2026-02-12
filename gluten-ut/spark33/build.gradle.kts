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
    id("gluten.scala-conventions")
    id("gluten.scalatest")
    id("gluten.spotless")
}

val scalaBinaryVersion: String by project
val backend: String by project

// Spark 3.3 specific versions
val sparkFullVersion = "3.3.1"

dependencies {
    implementation(project(":gluten-ut-common"))
    testImplementation(project(":gluten-ut-common", "testArtifacts"))
    implementation(project(":gluten-ut-test"))

    if (backend == "velox") {
        implementation(project(":backends-velox"))
        testImplementation(project(":backends-velox", "testArtifacts"))
    }

    compileOnly("org.apache.spark:spark-sql_$scalaBinaryVersion:$sparkFullVersion")
    compileOnly("org.apache.spark:spark-core_$scalaBinaryVersion:$sparkFullVersion")
    compileOnly("org.apache.spark:spark-catalyst_$scalaBinaryVersion:$sparkFullVersion")
    compileOnly("org.apache.spark:spark-hive_$scalaBinaryVersion:$sparkFullVersion")

    testImplementation("org.scalatest:scalatest_$scalaBinaryVersion:3.2.16")
    testImplementation("junit:junit:4.13.1")
    testImplementation("com.vladsch.flexmark:flexmark-all:0.62.2")

    testImplementation("org.apache.spark:spark-core_$scalaBinaryVersion:$sparkFullVersion:tests")
    testImplementation("org.apache.spark:spark-sql_$scalaBinaryVersion:$sparkFullVersion:tests")
    testImplementation("org.apache.spark:spark-catalyst_$scalaBinaryVersion:$sparkFullVersion:tests")
    testImplementation("org.apache.spark:spark-hive_$scalaBinaryVersion:$sparkFullVersion:tests")
}

// Add backend-specific test source directory
sourceSets {
    test {
        scala {
            srcDir("src/test/backends-$backend")
        }
    }
}
