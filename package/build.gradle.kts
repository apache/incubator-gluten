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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    id("gluten.java-conventions")
    id("gluten.shading")
    id("gluten.spotless")
}

val scalaBinaryVersion: String by project
val sparkVersion: String by project
val effectiveSparkFullVersion: String by rootProject.extra
val effectiveSparkPlainVersion: String by rootProject.extra
val backend: String by project
val glutenShadePackageName: String by project
val platform = rootProject.extra.get("platform") as String
val arch = rootProject.extra.get("arch") as String

// Dynamic dependencies based on backend selection
dependencies {
    // Spark (provided)
    compileOnly("org.apache.spark:spark-sql_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-core_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-catalyst_$scalaBinaryVersion:$effectiveSparkFullVersion")
    compileOnly("org.apache.spark:spark-hive_$scalaBinaryVersion:$effectiveSparkFullVersion")

    // Backend dependency
    if (backend == "velox") {
        implementation(project(":backends-velox"))
    } else if (backend == "clickhouse") {
        implementation(project(":backends-clickhouse"))
    }

    // Optional module dependencies
    val deltaEnabled = providers.gradleProperty("delta").getOrElse("false").toBoolean()
    val icebergEnabled = providers.gradleProperty("iceberg").getOrElse("false").toBoolean()
    val hudiEnabled = providers.gradleProperty("hudi").getOrElse("false").toBoolean()
    val paimonEnabled = providers.gradleProperty("paimon").getOrElse("false").toBoolean()
    val celebornEnabled = providers.gradleProperty("celeborn").getOrElse("false").toBoolean()
    val uniffleEnabled = providers.gradleProperty("uniffle").getOrElse("false").toBoolean()

    if (deltaEnabled) {
        implementation(project(":gluten-delta"))
    }
    if (icebergEnabled) {
        implementation(project(":gluten-iceberg"))
    }
    if (hudiEnabled) {
        implementation(project(":gluten-hudi"))
    }
    if (paimonEnabled) {
        implementation(project(":gluten-paimon"))
    }
    if (celebornEnabled) {
        implementation(project(":gluten-celeborn"))
    }
    if (uniffleEnabled) {
        implementation(project(":gluten-uniffle"))
    }
}

// Include build info from gluten-core
tasks.processResources {
    dependsOn(":gluten-core:generateBuildInfo")
    from("${project(":gluten-core").layout.buildDirectory.get()}/generated-resources") {
        include("gluten-build-info.properties")
    }
}

tasks.withType<ShadowJar>().configureEach {
    // Relocations
    relocate("com.google.protobuf", "$glutenShadePackageName.com.google.protobuf")
    relocate("com.google.common", "$glutenShadePackageName.com.google.common") {
        exclude("com.google.common.jimfs.**")
    }
    relocate("com.google.thirdparty", "$glutenShadePackageName.com.google.thirdparty")
    relocate("com.google.errorprone.annotations", "$glutenShadePackageName.com.google.errorprone.annotations")
    relocate("com.google.j2objc", "$glutenShadePackageName.com.google.j2objc")
    relocate("com.google.gson", "$glutenShadePackageName.com.google.gson")
    relocate("org.apache.arrow", "$glutenShadePackageName.org.apache.arrow") {
        exclude("org.apache.arrow.c.*")
        exclude("org.apache.arrow.c.jni.*")
        exclude("org.apache.arrow.dataset.**")
    }
    relocate("com.google.flatbuffers", "$glutenShadePackageName.com.google.flatbuffers")

    // Exclusions
    exclude("META-INF/*.SF")
    exclude("META-INF/*.DSA")
    exclude("META-INF/*.RSA")
    exclude("META-INF/DEPENDENCIES")
    exclude("META-INF/LICENSE.txt")
    exclude("META-INF/NOTICE.txt")
    exclude("LICENSE.txt")
    exclude("NOTICE.txt")
    exclude("gluten-build-info.properties") // Excluded from gluten-core, included from resources

    // Exclude findbugs annotations
    dependencies {
        exclude(dependency("com.google.code.findbugs:jsr305"))
    }

    // Merge service files
    mergeServiceFiles()

    // Output naming - match Maven convention
    val sparkBundleVersion = sparkVersion.replace(".", "")
    archiveBaseName.set("gluten-$backend-bundle-spark${sparkVersion}_$scalaBinaryVersion-${platform}_$arch")
    archiveVersion.set(project.version.toString())
    archiveClassifier.set("")
}

// Copy the shadow JAR with the Maven-compatible name
tasks.register<Copy>("copyBundleJar") {
    dependsOn(tasks.shadowJar)
    from(tasks.shadowJar.get().archiveFile)
    into(layout.buildDirectory.dir("libs"))
    rename { fileName ->
        "gluten-$backend-bundle-spark${sparkVersion}_$scalaBinaryVersion-${platform}_$arch-${project.version}.jar"
    }
}

tasks.build {
    dependsOn("copyBundleJar")
}
