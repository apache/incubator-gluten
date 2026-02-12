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
val protobufVersion: String by project
val effectiveSparkFullVersion: String by rootProject.extra
val effectiveHadoopVersion: String by rootProject.extra

dependencies {
    compileOnly(project(":gluten-substrait"))

    compileOnly("org.apache.spark:spark-sql-kafka-0-10_$scalaBinaryVersion:$effectiveSparkFullVersion")

    testImplementation(project(":gluten-substrait", "testArtifacts"))
    testImplementation("org.apache.spark:spark-hive_$scalaBinaryVersion:$effectiveSparkFullVersion")
    testImplementation("org.apache.hadoop:hadoop-client:$effectiveHadoopVersion")
    testImplementation("com.google.protobuf:protobuf-java:$protobufVersion")
}
