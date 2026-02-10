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

// The Maven build of this module does not enable strict Scala warnings, so the source
// code has unused imports, deprecated procedure syntax, etc. Suppress the strict flags
// that the convention plugin enables.
tasks.withType<ScalaCompile>().configureEach {
    scalaCompileOptions.additionalParameters =
        scalaCompileOptions.additionalParameters?.filter {
            it != "-Ywarn-unused:imports" && it != "-Wunused:imports" && !it.startsWith("-Wconf:")
        }
}

// The IT module's Java source code does not conform to the main project's checkstyle
// rules (line length, log4j imports). The Maven build doesn't run checkstyle here.
checkstyle {
    isIgnoreFailures = true
}

dependencies {
    // Shaded 3rd-party benchmark libs (exclude transitive deps since they're shaded)
    implementation(project(":gluten-it-3rd")) {
        isTransitive = false
    }

    // Gluten package (runtime — provides the bundle JAR)
    runtimeOnly(project(":gluten-package"))

    // Spark
    implementation("org.apache.spark:spark-core_$scalaBinaryVersion:$effectiveSparkFullVersion") {
        exclude(group = "org.apache.avro", module = "avro-mapred")
        exclude(group = "org.apache.avro", module = "avro")
    }
    implementation("org.apache.spark:spark-sql_$scalaBinaryVersion:$effectiveSparkFullVersion") {
        exclude(group = "com.google.protobuf", module = "protobuf-java")
    }
    implementation("org.apache.spark:spark-repl_$scalaBinaryVersion:$effectiveSparkFullVersion")
    implementation("org.apache.spark:spark-catalyst_$scalaBinaryVersion:$effectiveSparkFullVersion")
    implementation("org.apache.spark:spark-hive_$scalaBinaryVersion:$effectiveSparkFullVersion") {
        exclude(group = "jline", module = "jline")
    }

    // Spark test JARs (compile scope in Maven — needed at compile time)
    implementation("org.apache.spark:spark-core_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
    implementation("org.apache.spark:spark-catalyst_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
    implementation("org.apache.spark:spark-sql_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")

    // Scala collection compat
    implementation("org.scala-lang.modules:scala-collection-compat_$scalaBinaryVersion:2.12.0")

    // SLF4J simple logger
    implementation("org.slf4j:slf4j-simple:1.7.30")

    // CLI framework
    implementation("info.picocli:picocli:4.6.3")

    // Optional: Celeborn
    if (providers.gradleProperty("celeborn").getOrElse("false").toBoolean()) {
        val celebornVersion: String by project
        val sparkMajorVersion = if (effectiveSparkFullVersion.startsWith("4.")) "4" else "3"
        runtimeOnly("org.apache.celeborn:celeborn-client-spark-${sparkMajorVersion}-shaded_$scalaBinaryVersion:$celebornVersion")
    }

    // Optional: Uniffle
    if (providers.gradleProperty("uniffle").getOrElse("false").toBoolean()) {
        val uniffleVersion: String by project
        val sparkMajorVersion = if (effectiveSparkFullVersion.startsWith("4.")) "4" else "3"
        runtimeOnly("org.apache.uniffle:rss-client-spark${sparkMajorVersion}-shaded:$uniffleVersion")
    }

    // Optional: Delta
    if (providers.gradleProperty("delta").getOrElse("false").toBoolean()) {
        val effectiveDeltaPackageName: String? by rootProject.extra
        val effectiveDeltaVersion: String? by rootProject.extra
        val deltaPackage = effectiveDeltaPackageName ?: "delta-spark"
        val deltaVer = effectiveDeltaVersion ?: "4.0.0"
        runtimeOnly("io.delta:${deltaPackage}_$scalaBinaryVersion:$deltaVer") {
            exclude(group = "org.antlr")
            exclude(group = "org.scala-lang", module = "scala-library")
        }
    }
}
