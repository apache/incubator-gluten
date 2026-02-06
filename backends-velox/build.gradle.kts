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
val scalaVersion: String by project
val protobufVersion: String by project
val effectiveSparkFullVersion: String by rootProject.extra
val effectiveHadoopVersion: String by rootProject.extra
val effectiveArrowVersion: String by rootProject.extra

// C++ build directories
val cppBuildDir = file("../cpp/build")
val cppReleasesDir = file("$cppBuildDir/releases")

dependencies {
    // Project dependencies
    implementation(project(":gluten-substrait"))
    implementation(project(":gluten-arrow"))

    // Test JARs from other modules
    testImplementation(project(":gluten-substrait", "testArtifacts"))
    testImplementation(project(":gluten-ras-common", "testArtifacts"))

    // Protobuf
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")

    // Spark (provided)
    compileOnly("org.apache.spark:spark-sql_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-core_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-catalyst_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-network-common_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-hive_$scalaBinaryVersion:$effectiveSparkFullVersion")

    // Hadoop (provided)
    compileOnly("org.apache.hadoop:hadoop-client:$effectiveHadoopVersion")

    // Scala (provided)
    compileOnly("org.scala-lang:scala-library:$scalaVersion")
    implementation("org.scala-lang.modules:scala-collection-compat_$scalaBinaryVersion:2.11.0")

    // Jackson (provided)
    compileOnly("com.fasterxml.jackson.core:jackson-databind:2.18.2")
    compileOnly("com.fasterxml.jackson.core:jackson-annotations:2.18.2")
    compileOnly("com.fasterxml.jackson.core:jackson-core:2.18.2")
    compileOnly("com.fasterxml.jackson.module:jackson-module-scala_$scalaBinaryVersion:2.18.2")

    // Commons IO (provided)
    compileOnly("commons-io:commons-io:2.14.0")

    // Jimfs for file system testing
    implementation("com.google.jimfs:jimfs:1.3.0")

    // Test dependencies
    testImplementation("org.scalatest:scalatest_$scalaBinaryVersion:3.2.16")
    testImplementation("org.scalacheck:scalacheck_$scalaBinaryVersion:1.17.0")
    testImplementation("org.mockito:mockito-core:2.23.4") {
        exclude(group = "net.bytebuddy", module = "byte-buddy")
    }
    testImplementation("net.bytebuddy:byte-buddy:1.9.3")
    testImplementation("junit:junit:4.13.1")
    testImplementation("org.scalatestplus:scalatestplus-mockito_$scalaBinaryVersion:1.0.0-M2")
    testImplementation("org.scalatestplus:scalatestplus-scalacheck_$scalaBinaryVersion:3.1.0.0-RC2")
    testImplementation("com.github.javafaker:javafaker:1.0.2")
    testImplementation("com.vladsch.flexmark:flexmark-all:0.62.2")

    // Spark test JARs
    testImplementation("org.apache.spark:spark-core_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
    testImplementation("org.apache.spark:spark-sql_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
    testImplementation("org.apache.spark:spark-catalyst_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
    testImplementation("org.apache.spark:spark-hive_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")
    testImplementation("org.apache.spark:spark-tags_$scalaBinaryVersion:$effectiveSparkFullVersion:tests")

    // Spark common-utils needed for SparkBuildInfo (contains version info)
    testImplementation("org.apache.spark:spark-common-utils_$scalaBinaryVersion:$effectiveSparkFullVersion")

    // ScalaTest JUnit runner
    testRuntimeOnly("org.scalatestplus:junit-4-13_$scalaBinaryVersion:3.2.16.0")

    // JUnit 5 platform
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.9.3")
    testRuntimeOnly("org.junit.vintage:junit-vintage-engine:5.9.3")
}

// Configure protobuf compilation
protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
}

sourceSets {
    main {
        proto {
            srcDir("src/main/resources/org/apache/gluten/proto")
        }
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

// Configure ScalaTest
tasks.withType<Test>().configureEach {
    systemProperty("velox.udf.lib.path", "$cppBuildDir/velox/udf/examples/libmyudf.so,$cppBuildDir/velox/udf/examples/libmyudaf.so")
}
