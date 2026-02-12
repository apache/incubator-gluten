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
 * Applies the gradle-scalatest plugin to get ScalaTest's Runner on the classpath and
 * configures common test settings (JVM args, tag filtering, etc.).
 *
 * Test execution mode depends on the module:
 * - **gluten-ut modules**: Per-suite JVM isolation via [ScalaTestPerSuiteAction], which
 *   invokes `Runner -s <Suite>` in a separate `javaexec` per test suite class. This
 *   prevents Spark's JVM-global state (SparkContext, daemon threads) from leaking
 *   between suites.
 * - **All other modules**: Default gradle-scalatest plugin behavior (`Runner -R <dir>`),
 *   running all suites in a single JVM. This matches Maven's scalatest-maven-plugin
 *   behavior, where suites may share JVM state (e.g., native library loading).
 */

plugins {
    java
    id("com.github.maiflai.scalatest")
}

val scalaBinaryVersion: String by project

dependencies {
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
    maxParallelForks = 1

    // Increase memory for tests
    maxHeapSize = "4g"

    // Disable assertions to match Maven Surefire behavior.
    // Spark's UnsafeRow has debug assertions (e.g. sizeInBytes % 8 == 0 in setTotalSize)
    // that are not meant to be enforced at runtime. Gradle enables assertions by default
    // while Maven does not, causing spurious test failures.
    enableAssertions = false

    // Populate the gradle-scalatest plugin's `tags` extension from Gradle properties.
    // The plugin registers a PatternSet named "tags" on each Test task.
    // ScalaTestPerSuiteAction reads this extension to build -n/-l Runner flags.
    val tags = extensions.findByName("tags") as? org.gradle.api.tasks.util.PatternSet
    if (tags != null) {
        tagsToInclude?.split(",")?.map { it.trim() }?.let { tags.include(it) }
        tagsToExclude?.split(",")?.map { it.trim() }?.let { tags.exclude(it) }
    }

    // Spark test home directory
    if (sparkTestHome != null) {
        systemProperty("spark.test.home", sparkTestHome!!)
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

// Replace the gradle-scalatest plugin's action with per-suite forking for gluten-ut modules.
// Other modules keep the plugin's default single-JVM behavior (Runner -R <dir>), matching Maven.
// This must happen in afterEvaluate because the plugin sets its action during project evaluation.
val usePerSuiteForking = project.path.startsWith(":gluten-ut")
if (usePerSuiteForking) {
    afterEvaluate {
        tasks.withType<Test>().configureEach {
            val customAction = ScalaTestPerSuiteAction()
            actions.clear()
            doLast {
                customAction.execute(this as Test)
            }
        }
    }
}
