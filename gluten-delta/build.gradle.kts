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
val effectiveDeltaVersion: String? by rootProject.extra
val effectiveDeltaBinaryVersion: String? by rootProject.extra
val effectiveDeltaPackageName: String? by rootProject.extra

val deltaVersion = effectiveDeltaVersion ?: "4.0.0"
val deltaBinaryVersion = effectiveDeltaBinaryVersion ?: "40"
val deltaPackageName = effectiveDeltaPackageName ?: "delta-spark"

// Add Delta-specific source directories
sourceSets {
    main {
        scala {
            srcDir("src-delta/main/scala")
            srcDir("src-delta/main/java")
            srcDir("src-delta-spark$effectiveSparkPlainVersion/main/scala")
            srcDir("src-delta-spark$effectiveSparkPlainVersion/main/java")
            srcDir("src-delta$deltaBinaryVersion/main/scala")
            srcDir("src-delta$deltaBinaryVersion/main/java")
            srcDir("src-delta$deltaBinaryVersion-spark$effectiveSparkPlainVersion/main/scala")
            srcDir("src-delta$deltaBinaryVersion-spark$effectiveSparkPlainVersion/main/java")
        }
        resources {
            srcDir("src-delta/main/resources")
            srcDir("src-delta-spark$effectiveSparkPlainVersion/main/resources")
            srcDir("src-delta$deltaBinaryVersion/main/resources")
            srcDir("src-delta$deltaBinaryVersion-spark$effectiveSparkPlainVersion/main/resources")
        }
    }
    test {
        scala {
            srcDir("src-delta/test/scala")
            srcDir("src-delta/test/java")
            srcDir("src-delta-spark$effectiveSparkPlainVersion/test/scala")
            srcDir("src-delta-spark$effectiveSparkPlainVersion/test/java")
            srcDir("src-delta$deltaBinaryVersion/test/scala")
            srcDir("src-delta$deltaBinaryVersion/test/java")
            srcDir("src-delta$deltaBinaryVersion-spark$effectiveSparkPlainVersion/test/scala")
            srcDir("src-delta$deltaBinaryVersion-spark$effectiveSparkPlainVersion/test/java")
        }
        resources {
            srcDir("src-delta/test/resources")
            srcDir("src-delta-spark$effectiveSparkPlainVersion/test/resources")
            srcDir("src-delta$deltaBinaryVersion/test/resources")
            srcDir("src-delta$deltaBinaryVersion-spark$effectiveSparkPlainVersion/test/resources")
        }
    }
}

dependencies {
    implementation(project(":gluten-substrait"))

    compileOnly("io.delta:${deltaPackageName}_$scalaBinaryVersion:$deltaVersion") {
        exclude(group = "org.antlr")
        exclude(group = "org.scala-lang", module = "scala-library")
    }

    testImplementation(project(":backends-velox", "testArtifacts"))
    testImplementation(project(":gluten-substrait", "testArtifacts"))
    testImplementation("io.delta:${deltaPackageName}_$scalaBinaryVersion:$deltaVersion") {
        exclude(group = "org.antlr")
        exclude(group = "org.scala-lang", module = "scala-library")
    }
}
