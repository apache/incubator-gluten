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

/**
 * Convention plugin for ScalaTest testing.
 *
 * Uses JUnit 5 Platform with ScalaTest's JUnit engine so that Gradle's built-in
 * Test task handles test execution. This lets forkEvery work correctly — the old
 * gradle-scalatest plugin replaced the Test task with a single javaexec() call
 * that ran all suites in one JVM, ignoring forkEvery and causing SparkContext
 * leaks between suites.
 */

plugins {
    java
}

val scalaBinaryVersion: String by project
val scalatestVersion: String by project

dependencies {
    // ScalaTest JUnit 5 engine — registers as a JUnit Platform test engine
    // so Gradle discovers and runs ScalaTest suites via useJUnitPlatform().
    testRuntimeOnly("org.scalatestplus:junit-5-9_$scalaBinaryVersion:${scalatestVersion}.0")

    // Flexmark is required by ScalaTest for HTML report generation
    testRuntimeOnly("com.vladsch.flexmark:flexmark-all:0.62.2")
}

// ScalaTest tag filtering via Gradle properties:
//   -PtagsToInclude=org.apache.spark.tags.ExtendedSQLTest
//   -PtagsToExclude=org.apache.gluten.tags.UDFTest,org.apache.gluten.tags.SkipTest
// Spark test home:
//   -PsparkTestHome=/opt/shims/spark35/spark_home/
val tagsToInclude: String? = providers.gradleProperty("tagsToInclude").orNull
val tagsToExclude: String? = providers.gradleProperty("tagsToExclude").orNull
val sparkTestHome: String? = providers.gradleProperty("sparkTestHome").orNull
val testJvmArgs: String? = providers.gradleProperty("testJvmArgs").orNull

tasks.withType<Test>().configureEach {
    // Use JUnit 5 Platform with ScalaTest engine.
    // This respects forkEvery because Gradle's Test task handles forking.
    useJUnitPlatform {
        includeEngines("scalatest")

        // ScalaTest tag filtering (maps to JUnit 5 Platform includeTags/excludeTags)
        tagsToInclude?.split(",")?.map { it.trim() }?.let { includeTags(*it.toTypedArray()) }
        tagsToExclude?.split(",")?.map { it.trim() }?.let { excludeTags(*it.toTypedArray()) }
    }

    // Exclude abstract classes, traits, Scala companion objects, and inner classes.
    // The ScalaTest JUnit 5 engine can't instantiate them and reports failures or
    // produces noisy "discovered suite count: 0" logs.
    exclude(AbstractClassExcludeSpec())
    exclude("**/*\$*.class")

    maxParallelForks = 1

    // Fork a new JVM for each test class to match Maven's scalatest-maven-plugin behavior.
    // Spark tests leak JVM-global state (SparkContext, daemon threads, etc.) between suites.
    // Without isolation, a leaked SparkContext from one suite causes NPEs in the next suite's
    // BlockManager initialization.
    forkEvery = 1

    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = true
        showExceptions = true
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }

    // Increase memory for tests
    maxHeapSize = "4g"

    // Disable assertions to match Maven Surefire behavior.
    // Spark's UnsafeRow has debug assertions (e.g. sizeInBytes % 8 == 0 in setTotalSize)
    // that are not meant to be enforced at runtime. Gradle enables assertions by default
    // while Maven does not, causing spurious test failures.
    enableAssertions = false

    // Spark test home directory
    if (sparkTestHome != null) {
        systemProperty("spark.test.home", sparkTestHome)
    }

    // Extra JVM args from CI (comma-separated -D flags)
    testJvmArgs?.split(",")?.forEach { arg ->
        if (arg.startsWith("-D") && arg.contains("=")) {
            val key = arg.substringAfter("-D").substringBefore("=")
            val value = arg.substringAfter("=")
            systemProperty(key, value)
        }
    }

    // JVM arguments for Spark tests
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
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "-Djdk.reflect.useDirectMethodHandle=false"
    )
}
