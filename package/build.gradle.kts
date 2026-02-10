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
val backend: String by project
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
    mapOf(
        "delta" to ":gluten-delta",
        "iceberg" to ":gluten-iceberg",
        "hudi" to ":gluten-hudi",
        "paimon" to ":gluten-paimon",
        "celeborn" to ":gluten-celeborn",
        "uniffle" to ":gluten-uniffle",
        "kafka" to ":gluten-kafka",
    ).forEach { (property, module) ->
        if (providers.gradleProperty(property).getOrElse("false").toBoolean()) {
            implementation(project(module))
        }
    }
}

// Include build info from gluten-core
tasks.processResources {
    dependsOn(":gluten-core:generateBuildInfo")
    from("${project(":gluten-core").layout.buildDirectory.get()}/generated-resources") {
        include("gluten-build-info.properties")
    }
}

// The gluten.shading plugin provides relocations, META-INF exclusions, and mergeServiceFiles.
// Here we add package-specific configuration on top.
tasks.withType<ShadowJar>().configureEach {
    exclude("gluten-build-info.properties") // Excluded from gluten-core, included from resources

    dependencies {
        exclude(dependency("com.google.code.findbugs:jsr305"))
    }

    // Output naming - match Maven convention
    archiveBaseName.set("gluten-$backend-bundle-spark${sparkVersion}_$scalaBinaryVersion-${platform}_$arch")
    archiveVersion.set(project.version.toString())
}

// Copy the shadow JAR with the Maven-compatible name
tasks.register<Copy>("copyBundleJar") {
    dependsOn(tasks.shadowJar)
    from(tasks.shadowJar.get().archiveFile)
    into(layout.buildDirectory.dir("libs"))
    rename { _ ->
        "gluten-$backend-bundle-spark${sparkVersion}_$scalaBinaryVersion-${platform}_$arch-${project.version}.jar"
    }
}

tasks.build {
    dependsOn("copyBundleJar")
}

// This is an assembly module (fat JAR); skip Maven publishing.
tasks.withType<PublishToMavenLocal>().configureEach { enabled = false }
tasks.withType<PublishToMavenRepository>().configureEach { enabled = false }
