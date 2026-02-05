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

pluginManagement {
    includeBuild("gradle/plugins")
    repositories {
        gradlePluginPortal()
        mavenCentral()
        maven {
            url = uri("https://maven-central.storage-download.googleapis.com/maven2/")
        }
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.PREFER_SETTINGS)
    repositories {
        mavenCentral()
        maven {
            url = uri("https://maven-central.storage-download.googleapis.com/maven2/")
        }
    }
}

rootProject.name = "gluten"

// Core modules
include("gluten-core")
include("gluten-substrait")
include("gluten-ui")

// RAS modules
include("gluten-ras-common")
project(":gluten-ras-common").projectDir = file("gluten-ras/common")

include("gluten-ras-planner")
project(":gluten-ras-planner").projectDir = file("gluten-ras/planner")

// Shims - common is always included
include("shims-common")
project(":shims-common").projectDir = file("shims/common")

// Version-specific shims - included based on sparkVersion property
val sparkVersion = providers.gradleProperty("sparkVersion").getOrElse("4.1")
val sparkPlainVersion = when (sparkVersion) {
    "3.3" -> "33"
    "3.4" -> "34"
    "3.5" -> "35"
    "4.0" -> "40"
    "4.1" -> "41"
    else -> "41"
}

include("shims-spark${sparkPlainVersion}")
project(":shims-spark${sparkPlainVersion}").projectDir = file("shims/spark${sparkPlainVersion}")

// Backend selection
val backend = providers.gradleProperty("backend").getOrElse("velox")

if (backend == "velox") {
    include("gluten-arrow")
    include("backends-velox")
}

if (backend == "clickhouse") {
    include("backends-clickhouse")
}

// Optional feature modules
val deltaEnabled = providers.gradleProperty("delta").getOrElse("false").toBoolean()
val icebergEnabled = providers.gradleProperty("iceberg").getOrElse("false").toBoolean()
val hudiEnabled = providers.gradleProperty("hudi").getOrElse("false").toBoolean()
val paimonEnabled = providers.gradleProperty("paimon").getOrElse("false").toBoolean()
val celebornEnabled = providers.gradleProperty("celeborn").getOrElse("false").toBoolean()
val uniffleEnabled = providers.gradleProperty("uniffle").getOrElse("false").toBoolean()

if (deltaEnabled) {
    include("gluten-delta")
}

if (icebergEnabled) {
    include("gluten-iceberg")
}

if (hudiEnabled) {
    include("gluten-hudi")
}

if (paimonEnabled) {
    include("gluten-paimon")
}

if (celebornEnabled) {
    include("gluten-celeborn")
}

if (uniffleEnabled) {
    include("gluten-uniffle")
}

// Package module (shadow JAR assembly)
include("gluten-package")
project(":gluten-package").projectDir = file("package")

// Unit test modules
include("gluten-ut-common")
project(":gluten-ut-common").projectDir = file("gluten-ut/common")

include("gluten-ut-test")
project(":gluten-ut-test").projectDir = file("gluten-ut/test")

// Spark version-specific UT modules
include("gluten-ut-spark${sparkPlainVersion}")
project(":gluten-ut-spark${sparkPlainVersion}").projectDir = file("gluten-ut/spark${sparkPlainVersion}")
