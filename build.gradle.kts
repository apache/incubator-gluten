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
    base
    id("gluten.spotless")
}

group = "org.apache.gluten"
version = providers.gradleProperty("glutenVersion").getOrElse("1.6.0-SNAPSHOT")

val sparkVersion: String by project
val sparkFullVersion: String by project
val sparkPlainVersion: String by project
val javaVersion: String by project
val backend: String by project

// Spark 3.x defaults to Scala 2.12, Spark 4.x defaults to Scala 2.13.
// An explicit -PscalaBinaryVersion on the command line always wins because
// Gradle command-line properties override gradle.properties values.
val defaultScalaBinaryVersion = if (sparkVersion.startsWith("3.")) "2.12" else "2.13"
val scalaBinaryVersion: String =
    providers.gradleProperty("scalaBinaryVersion").getOrElse(defaultScalaBinaryVersion)
// Derive full Scala version from binary version
val scalaVersion: String =
    when (scalaBinaryVersion) {
        "2.12" -> "2.12.18"
        "2.13" -> providers.gradleProperty("scalaVersion").getOrElse("2.13.17")
        else -> providers.gradleProperty("scalaVersion").getOrElse("2.13.17")
    }

// Version properties for dependencies
val arrowVersion: String by project
val hadoopVersion: String by project
val caffeineVersion: String =
    if ((javaVersion.toIntOrNull() ?: 17) >= 11) {
        providers.gradleProperty("caffeineVersion").getOrElse("3.1.8")
    } else {
        "2.9.3"
    }

// Computed properties based on Spark version
val sparkProperties =
    when (sparkVersion) {
        "3.3" ->
            mapOf(
                "sparkFullVersion" to "3.3.1",
                "sparkPlainVersion" to "33",
                "deltaVersion" to "2.3.0",
                "deltaBinaryVersion" to "23",
                "deltaPackageName" to "delta-core",
                "icebergVersion" to "1.5.0",
                "icebergBinaryVersion" to "5",
                "antlr4Version" to "4.8",
                "hudiVersion" to "0.15.0",
            )
        "3.4" ->
            mapOf(
                "sparkFullVersion" to "3.4.4",
                "sparkPlainVersion" to "34",
                "deltaVersion" to "2.4.0",
                "deltaBinaryVersion" to "24",
                "deltaPackageName" to "delta-core",
                "icebergVersion" to "1.10.0",
                "icebergBinaryVersion" to "10",
                "antlr4Version" to "4.9.3",
                "hudiVersion" to "0.15.0",
            )
        "3.5" ->
            mapOf(
                "sparkFullVersion" to "3.5.5",
                "sparkPlainVersion" to "35",
                "deltaVersion" to "3.3.2",
                "deltaBinaryVersion" to "33",
                "deltaPackageName" to "delta-spark",
                "icebergVersion" to "1.10.0",
                "icebergBinaryVersion" to "10",
                "antlr4Version" to "4.9.3",
                "hudiVersion" to "0.15.0",
                "hadoopVersion" to "3.3.4",
            )
        "4.0" ->
            mapOf(
                "sparkFullVersion" to "4.0.1",
                "sparkPlainVersion" to "40",
                "deltaVersion" to "4.0.0",
                "deltaBinaryVersion" to "40",
                "deltaPackageName" to "delta-spark",
                "icebergVersion" to "1.10.0",
                "icebergBinaryVersion" to "10",
                "antlr4Version" to "4.13.1",
                "hudiVersion" to "1.1.0",
                "paimonVersion" to "1.3.0",
                "hadoopVersion" to "3.4.1",
                "arrowVersion" to "18.1.0",
            )
        // "4.1" and default
        else ->
            mapOf(
                "sparkFullVersion" to "4.1.1",
                "sparkPlainVersion" to "41",
                "deltaVersion" to "4.0.0",
                "deltaBinaryVersion" to "40",
                "deltaPackageName" to "delta-spark",
                "icebergVersion" to "1.10.0",
                "icebergBinaryVersion" to "10",
                "antlr4Version" to "4.13.1",
                "hudiVersion" to "1.1.0",
                "paimonVersion" to "1.3.0",
                "hadoopVersion" to "3.4.1",
                "arrowVersion" to "18.1.0",
            )
    }

// Make computed properties available to subprojects
extra["effectiveSparkFullVersion"] = sparkProperties["sparkFullVersion"] ?: sparkFullVersion
extra["effectiveSparkPlainVersion"] = sparkProperties["sparkPlainVersion"] ?: sparkPlainVersion
extra["effectiveHadoopVersion"] = sparkProperties["hadoopVersion"] ?: hadoopVersion
extra["effectiveArrowVersion"] = sparkProperties["arrowVersion"] ?: arrowVersion
extra["effectiveDeltaVersion"] = sparkProperties["deltaVersion"]
extra["effectiveDeltaBinaryVersion"] = sparkProperties["deltaBinaryVersion"]
extra["effectiveDeltaPackageName"] = sparkProperties["deltaPackageName"]
extra["effectiveIcebergVersion"] = sparkProperties["icebergVersion"]
extra["effectiveIcebergBinaryVersion"] = sparkProperties["icebergBinaryVersion"]
extra["effectiveHudiVersion"] = sparkProperties["hudiVersion"]
extra["effectivePaimonVersion"] = sparkProperties["paimonVersion"]

// Detect OS and architecture
val osName = System.getProperty("os.name").lowercase()
val osArch = System.getProperty("os.arch")

val platform =
    when {
        osName.contains("linux") -> "linux"
        osName.contains("mac") || osName.contains("darwin") -> "darwin"
        osName.contains("windows") -> "windows"
        else -> "unknown"
    }

val arch =
    when (osArch) {
        "amd64", "x86_64" -> "amd64"
        "aarch64", "arm64" -> "arm64"
        else -> osArch
    }

extra["platform"] = platform
extra["arch"] = arch

// Validate version compatibility
if (sparkVersion in listOf("4.0", "4.1")) {
    val javaVersionInt = javaVersion.toIntOrNull() ?: 17
    require(javaVersionInt >= 17) {
        "Spark $sparkVersion requires Java 17+, but javaVersion=$javaVersion"
    }
    require(scalaBinaryVersion == "2.13") {
        "Spark $sparkVersion requires Scala 2.13, but scalaBinaryVersion=$scalaBinaryVersion"
    }
}

allprojects {
    group = rootProject.group
    version = rootProject.version
    // Override computed properties for all projects so convention plugins pick up the correct values
    extra["scalaBinaryVersion"] = scalaBinaryVersion
    extra["scalaVersion"] = scalaVersion
    extra["caffeineVersion"] = caffeineVersion
}

subprojects {
    // jackson-bom:2.13.4.1 (pulled by Spark 3.3's jackson-databind:2.13.4.1) does not
    // exist on Maven Central. Substitute it with 2.13.5 which is the closest available version.
    configurations.all {
        resolutionStrategy.eachDependency {
            if (requested.group == "com.fasterxml.jackson" && requested.name == "jackson-bom" && requested.version == "2.13.4.1") {
                useVersion("2.13.5")
                because("jackson-bom:2.13.4.1 does not exist on Maven Central")
            }
        }
    }

    // Configure test tasks
    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        maxHeapSize = "4g"

        jvmArgs(
            "-XX:+IgnoreUnrecognizedVMOptions",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
            "--add-opens=java.base/java.io=ALL-UNNAMED",
            "--add-opens=java.base/java.net=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/java.util=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
            "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
            "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
            "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
            "-Djdk.reflect.useDirectMethodHandle=false",
            "-Dio.netty.tryReflectionSetAccessible=true",
        )

        testLogging {
            events("passed", "skipped", "failed")
            showStandardStreams = false
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        }
    }
}

// Task to print build configuration
tasks.register("printConfig") {
    group = "help"
    description = "Print the current build configuration"
    doLast {
        println(
            """
            |=== Gluten Build Configuration ===
            |Version: ${project.version}
            |Scala: $scalaVersion (binary: $scalaBinaryVersion)
            |Spark: $sparkVersion (full: ${project.extra["effectiveSparkFullVersion"]})
            |Java: $javaVersion
            |Backend: $backend
            |Platform: $platform
            |Architecture: $arch
            |
            |Optional modules:
            |  delta: ${providers.gradleProperty("delta").getOrElse("false")}
            |  iceberg: ${providers.gradleProperty("iceberg").getOrElse("false")}
            |  hudi: ${providers.gradleProperty("hudi").getOrElse("false")}
            |  paimon: ${providers.gradleProperty("paimon").getOrElse("false")}
            |  celeborn: ${providers.gradleProperty("celeborn").getOrElse("false")}
            |  uniffle: ${providers.gradleProperty("uniffle").getOrElse("false")}
            """.trimMargin(),
        )
    }
}
