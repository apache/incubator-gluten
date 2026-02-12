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
    id("gluten.protobuf")
    id("gluten.spotless")
    id("gluten.scalatest")
}

val scalaBinaryVersion: String by project
val protobufVersion: String by project
val effectiveSparkFullVersion: String by rootProject.extra
val effectiveSparkPlainVersion: String by rootProject.extra
val effectiveHadoopVersion: String by rootProject.extra
val effectiveArrowVersion: String by rootProject.extra
val effectiveFasterxmlVersion: String by rootProject.extra

// C++ build directories
val cppBuildDir = file("../cpp/build")
val cppReleasesDir = file("$cppBuildDir/releases")

dependencies {
    implementation(project(":gluten-substrait"))
    implementation(project(":gluten-arrow"))

    testImplementation(project(":gluten-substrait", "testArtifacts"))
    testImplementation(project(":gluten-ras-common", "testArtifacts"))

    implementation("com.google.protobuf:protobuf-java:$protobufVersion")

    compileOnly("org.apache.spark:spark-network-common_$scalaBinaryVersion:$effectiveSparkFullVersion")

    compileOnly("org.apache.hadoop:hadoop-client:$effectiveHadoopVersion")

    implementation("org.scala-lang.modules:scala-collection-compat_$scalaBinaryVersion:2.11.0")

    compileOnly("com.fasterxml.jackson.core:jackson-databind:$effectiveFasterxmlVersion")
    compileOnly("com.fasterxml.jackson.core:jackson-annotations:$effectiveFasterxmlVersion")
    compileOnly("com.fasterxml.jackson.core:jackson-core:$effectiveFasterxmlVersion")
    compileOnly("com.fasterxml.jackson.module:jackson-module-scala_$scalaBinaryVersion:$effectiveFasterxmlVersion")

    compileOnly("commons-io:commons-io:2.14.0")

    implementation("com.google.jimfs:jimfs:1.3.0")

    testImplementation("org.scalacheck:scalacheck_$scalaBinaryVersion:1.17.0")
    testImplementation("org.mockito:mockito-core:2.23.4") {
        exclude(group = "net.bytebuddy", module = "byte-buddy")
    }
    testImplementation("net.bytebuddy:byte-buddy:1.9.3")
    testImplementation("org.scalatestplus:scalatestplus-mockito_$scalaBinaryVersion:1.0.0-M2")
    testImplementation("org.scalatestplus:scalatestplus-scalacheck_$scalaBinaryVersion:3.1.0.0-RC2")
    testImplementation("com.github.javafaker:javafaker:1.0.2")
    testImplementation("com.vladsch.flexmark:flexmark-all:0.62.2")

    testImplementation("org.apache.spark:spark-tags_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
}

sourceSets {
    main {
        proto {
            srcDir("src/main/resources/org/apache/gluten/proto")
        }
    }
}

// Celeborn: add src-celeborn source dirs and dependencies when enabled
if (providers.gradleProperty("celeborn").getOrElse("false").toBoolean()) {
    val celebornVersion: String by project
    val sparkMajorVersion = if (effectiveSparkFullVersion.startsWith("4.")) "4" else "3"
    sourceSets {
        main {
            scala {
                srcDir("src-celeborn/main/scala")
                srcDir("src-celeborn/main/java")
                srcDir("src-celeborn-spark$effectiveSparkPlainVersion/main/scala")
                srcDir("src-celeborn-spark$effectiveSparkPlainVersion/main/java")
            }
            resources {
                srcDir("src-celeborn/main/resources")
                srcDir("src-celeborn-spark$effectiveSparkPlainVersion/main/resources")
            }
        }
    }
    dependencies {
        implementation(project(":gluten-celeborn"))
        compileOnly("org.apache.celeborn:celeborn-client-spark-${sparkMajorVersion}-shaded_$scalaBinaryVersion:$celebornVersion")
    }
}

// Uniffle: add src-uniffle source dirs and dependencies when enabled
if (providers.gradleProperty("uniffle").getOrElse("false").toBoolean()) {
    val uniffleVersion: String by project
    val sparkMajorVersion = if (effectiveSparkFullVersion.startsWith("4.")) "4" else "3"
    sourceSets {
        main {
            scala {
                srcDir("src-uniffle/main/scala")
                srcDir("src-uniffle/main/java")
                srcDir("src-uniffle-spark$effectiveSparkPlainVersion/main/scala")
                srcDir("src-uniffle-spark$effectiveSparkPlainVersion/main/java")
            }
            resources {
                srcDir("src-uniffle/main/resources")
                srcDir("src-uniffle-spark$effectiveSparkPlainVersion/main/resources")
            }
        }
    }
    dependencies {
        implementation(project(":gluten-uniffle"))
        compileOnly("org.apache.uniffle:rss-client-spark${sparkMajorVersion}-shaded:$uniffleVersion")
    }
}

// ============================================================
// Native C++ build tasks
// ============================================================

val glutenDir = rootProject.projectDir
val veloxHome =
    providers.gradleProperty("veloxHome")
        .getOrElse("$glutenDir/ep/build-velox/build/velox_ep")
val buildType = providers.gradleProperty("nativeBuildType").getOrElse("Release")
val numThreads =
    providers.gradleProperty("nativeThreads")
        .getOrElse(Runtime.getRuntime().availableProcessors().toString())

// Feature flags for native build (match dev/builddeps-veloxbe.sh defaults)
val enableHdfs = providers.gradleProperty("enableHdfs").getOrElse("OFF")
val enableS3 = providers.gradleProperty("enableS3").getOrElse("OFF")
val enableGcs = providers.gradleProperty("enableGcs").getOrElse("OFF")
val enableAbfs = providers.gradleProperty("enableAbfs").getOrElse("OFF")
val enableQat = providers.gradleProperty("enableQat").getOrElse("OFF")
val enableGpu = providers.gradleProperty("enableGpu").getOrElse("OFF")
val buildTests = providers.gradleProperty("nativeBuildTests").getOrElse("OFF")
val buildBenchmarks = providers.gradleProperty("nativeBuildBenchmarks").getOrElse("OFF")

// Task 1: Fetch Velox source code
val getVelox by tasks.registering(Exec::class) {
    group = "native"
    description = "Fetch Velox source code"

    workingDir = file("$glutenDir/ep/build-velox/src")

    commandLine(
        "bash",
        "./get-velox.sh",
        "--velox_home=$veloxHome",
        "--run_setup_script=OFF",
    )

    // Skip if velox source already present with correct commit
    onlyIf {
        !file("$veloxHome/CMakeLists.txt").exists()
    }
}

// Task 2: Build Velox library
val buildVelox by tasks.registering(Exec::class) {
    group = "native"
    description = "Build the Velox C++ library"
    dependsOn(getVelox)

    workingDir = file("$glutenDir/ep/build-velox/src")

    commandLine(
        "bash", "./build-velox.sh",
        "--build_type=$buildType",
        "--velox_home=$veloxHome",
        "--enable_hdfs=$enableHdfs",
        "--enable_s3=$enableS3",
        "--enable_gcs=$enableGcs",
        "--enable_abfs=$enableAbfs",
        "--enable_gpu=$enableGpu",
        "--build_test_utils=$buildTests",
        "--num_threads=$numThreads",
    )

    // Skip if Velox is already built
    onlyIf {
        val buildDir = if (buildType == "Debug") "debug" else "release"
        !file("$veloxHome/_build/$buildDir/lib/libvelox.a").exists()
    }
}

// Task 3: Configure CMake for Gluten C++
val configureNative by tasks.registering(Exec::class) {
    group = "native"
    description = "Configure CMake for Gluten C++ build"
    dependsOn(buildVelox)

    workingDir = cppBuildDir

    doFirst {
        cppBuildDir.mkdirs()
    }

    commandLine(
        "cmake",
        "-DBUILD_VELOX_BACKEND=ON",
        "-DCMAKE_BUILD_TYPE=$buildType",
        "-DVELOX_HOME=$veloxHome",
        "-DBUILD_TESTS=$buildTests",
        "-DBUILD_BENCHMARKS=$buildBenchmarks",
        "-DENABLE_HDFS=$enableHdfs",
        "-DENABLE_S3=$enableS3",
        "-DENABLE_GCS=$enableGcs",
        "-DENABLE_ABFS=$enableAbfs",
        "-DENABLE_QAT=$enableQat",
        "-DENABLE_GPU=$enableGpu",
        "-DCMAKE_EXPORT_COMPILE_COMMANDS=ON",
        "..",
    )

    // Skip if CMake is already configured
    onlyIf {
        !file("$cppBuildDir/CMakeCache.txt").exists()
    }
}

// Task 4: Build Gluten C++ (libgluten.so + libvelox.so)
val buildNative by tasks.registering(Exec::class) {
    group = "native"
    description = "Build native C++ libraries (libgluten.so + libvelox.so)"
    dependsOn(configureNative)

    workingDir = file("../cpp")

    commandLine(
        "cmake",
        "--build",
        "build",
        "--parallel",
        numThreads,
    )

    outputs.dir(cppReleasesDir)
}

// Convenience task: full native build pipeline
val buildNativeAll by tasks.registering {
    group = "native"
    description = "Full native build: fetch Velox + build Velox + configure + build Gluten C++"
    dependsOn(buildNative)
}

// Include native libraries in the JAR
val platform = rootProject.extra.get("platform") as String
val arch = rootProject.extra.get("arch") as String

tasks.processResources {
    from(cppReleasesDir) {
        into("$platform/$arch")
    }
}

// Bridge path differences between Maven and Gradle test output layouts.
// Test code uses getClass.getResource("/").getPath + "../../../../tools/..." to locate
// shared resources. Maven's classpath root is target/test-classes/ (2 levels deep),
// so ../../../../ goes to project root. Gradle's is build/classes/scala/test/ (4 levels
// deep), so ../../../../ only reaches the module root. Create a symlink so the traversal
// resolves correctly from Gradle's deeper output directory.
// Note: build/src symlink is handled by gluten.scala-conventions plugin.
val createTestPathSymlinks by tasks.registering {
    val toolsLink = file("tools")
    outputs.upToDateWhen { toolsLink.exists() }
    doLast {
        // backends-velox/tools -> ../tools (for ../../../../tools/... from build/classes/scala/test/)
        if (!toolsLink.exists()) {
            exec { commandLine("ln", "-s", "../tools", toolsLink.absolutePath) }
        }
    }
}

// Configure ScalaTest
tasks.withType<Test>().configureEach {
    dependsOn(createTestPathSymlinks)
    systemProperty("velox.udf.lib.path", "$cppBuildDir/velox/udf/examples/libmyudf.so,$cppBuildDir/velox/udf/examples/libmyudaf.so")

    // Reorder test classpath so that project (shim) classes appear before external JARs.
    // Spark 3.3's native write path requires the shim's FileFormatWriter to shadow Spark's
    // own version. Maven achieves this via dependency declaration order (compile-scoped shim
    // before provided-scoped Spark). Gradle's compileOnly→testImplementation inheritance
    // places Spark JARs first. Fix by partitioning the classpath: project outputs first,
    // then external JARs.
    val rootPath = rootProject.projectDir.absolutePath
    doFirst {
        val (projectFiles, externalFiles) = classpath.partition {
            it.absolutePath.startsWith(rootPath)
        }
        classpath = files(projectFiles, externalFiles)
    }
}
