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
 * Convention plugin for Scala compilation settings.
 * Configures Scala compiler with WartRemover and other options.
 */

plugins {
    scala
    id("gluten.java-conventions")
}

val scalaVersion: String by project
val scalaBinaryVersion: String by project

dependencies {
    // Scala library
    implementation("org.scala-lang:scala-library:$scalaVersion")

    // WartRemover compiler plugin
    val wartremoverVersion = if (scalaBinaryVersion == "2.13") "3.2.0" else "3.0.6"
    scalaCompilerPlugins("org.wartremover:wartremover_$scalaBinaryVersion:$wartremoverVersion")
}

tasks.withType<ScalaCompile>().configureEach {
    scalaCompileOptions.apply {
        encoding = "UTF-8"

        // Common compiler arguments
        val commonArgs = mutableListOf(
            "-deprecation",
            "-feature",
            "-unchecked"
        )

        // WartRemover configuration
        commonArgs.add("-P:wartremover:traverser:io.github.zhztheplayer.scalawarts.InheritFromCaseClass")

        if (scalaBinaryVersion == "2.13") {
            // Scala 2.13 specific arguments
            commonArgs.addAll(listOf(
                "-explaintypes",
                "-Wconf:cat=deprecation:wv,any:e",
                "-Wunused:imports",
                "-Wconf:cat=scaladoc:wv",
                "-Wconf:cat=other-nullary-override:wv",
                "-Wconf:msg=^(?=.*?method|value|type|object|trait|inheritance)(?=.*?deprecated)(?=.*?since 2.13).+$:s",
                "-Wconf:msg=^(?=.*?Widening conversion from)(?=.*?is deprecated because it loses precision).+$:s",
                "-Wconf:msg=Auto-application to \\`\\(\\)\\` is deprecated:s",
                "-Wconf:msg=method with a single empty parameter list overrides method without any parameter list:s",
                "-Wconf:msg=method without a parameter list overrides a method with a single empty one:s",
                "-Wconf:cat=deprecation&msg=procedure syntax is deprecated:e",
                "-Wconf:cat=unchecked&msg=outer reference:s",
                "-Wconf:cat=unchecked&msg=eliminated by erasure:s",
                "-Wconf:msg=^(?=.*?a value of type)(?=.*?cannot also be).+$:s"
            ))
        } else {
            // Scala 2.12 specific arguments
            commonArgs.addAll(listOf(
                "-Wconf:msg=While parsing annotations in:silent,any:e",
                "-Ywarn-unused:imports",
                "-Wconf:cat=deprecation:wv,any:e"
            ))
        }

        additionalParameters = commonArgs
    }
    // Set encoding for the forked javac process that compiles Java sources
    // during mixed Scala/Java compilation (zinc forks javac separately)
    options.encoding = "UTF-8"
    options.isFork = true
    options.forkOptions.jvmArgs = (options.forkOptions.jvmArgs ?: mutableListOf()).apply {
        add("-Dfile.encoding=UTF-8")
    }
}

// Configure mixed Scala/Java compilation
// When using mixed sources, Scala compiler handles both .scala and .java files
// to ensure proper compilation order and cross-referencing
sourceSets {
    main {
        scala {
            srcDirs("src/main/scala", "src/main/java")
        }
        // Remove Java sources from Java source set to avoid double compilation
        java {
            setSrcDirs(emptyList<File>())
        }
    }
    test {
        scala {
            srcDirs("src/test/scala", "src/test/java")
        }
        java {
            setSrcDirs(emptyList<File>())
        }
    }
}

// Handle potential duplicate class files
tasks.withType<Jar>().configureEach {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
