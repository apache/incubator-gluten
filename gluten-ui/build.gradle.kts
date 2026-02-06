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

dependencies {
    // Project dependencies
    compileOnly(project(":shims-common"))
    implementation(project(":shims-spark$effectiveSparkPlainVersion"))

    // Spark (provided)
    compileOnly("org.apache.spark:spark-sql_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-core_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-catalyst_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-kvstore_$scalaBinaryVersion:$effectiveSparkFullVersion")

    // Scala (provided)
    compileOnly("org.scala-lang:scala-library:$scalaVersion")

    // Servlet API (provided)
    compileOnly("javax.servlet:javax.servlet-api:3.1.0")

    // Jackson (provided)
    compileOnly("com.fasterxml.jackson.core:jackson-databind:2.18.2")
    compileOnly("com.fasterxml.jackson.core:jackson-annotations:2.18.2")
    compileOnly("com.fasterxml.jackson.core:jackson-core:2.18.2")

    // Scala XML (provided)
    compileOnly("org.scala-lang.modules:scala-xml_$scalaBinaryVersion:2.2.0")

    // Test dependencies
    testImplementation("org.scalatest:scalatest_$scalaBinaryVersion:3.2.16")
    testImplementation("junit:junit:4.13.1")
}
