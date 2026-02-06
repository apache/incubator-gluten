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
    id("gluten.spotless")
}

val scalaBinaryVersion: String by project
val effectiveSparkFullVersion: String by rootProject.extra

dependencies {
    // Project dependencies
    implementation(project(":gluten-substrait"))

    // Spark (provided)
    compileOnly("org.apache.spark:spark-sql_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-core_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-catalyst_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-hive_$scalaBinaryVersion:$effectiveSparkFullVersion")

    // Test dependencies
    implementation("org.scalatest:scalatest_$scalaBinaryVersion:3.2.16")
    implementation("junit:junit:4.13.1")
    implementation("com.vladsch.flexmark:flexmark-all:0.62.2")

    // Spark test JARs
    implementation("org.apache.spark:spark-core_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
    implementation("org.apache.spark:spark-sql_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
    implementation("org.apache.spark:spark-catalyst_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
    implementation("org.apache.spark:spark-hive_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
}
