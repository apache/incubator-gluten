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

// Spark 3.3 specific versions
val sparkFullVersion = "3.3.1"
val hadoopVersion = "2.7.4"

dependencies {
    implementation(project(":shims-common"))

    compileOnly("org.apache.spark:spark-sql_$scalaBinaryVersion:$sparkFullVersion")
    compileOnly("org.apache.spark:spark-core_$scalaBinaryVersion:$sparkFullVersion")
    compileOnly("org.apache.spark:spark-catalyst_$scalaBinaryVersion:$sparkFullVersion")
    compileOnly("org.apache.spark:spark-hive_$scalaBinaryVersion:$sparkFullVersion")

    compileOnly("org.apache.hadoop:hadoop-client:$hadoopVersion")

    testImplementation("org.scalatest:scalatest_$scalaBinaryVersion:3.2.16")
    testImplementation("org.slf4j:slf4j-log4j12:1.7.30")
}
