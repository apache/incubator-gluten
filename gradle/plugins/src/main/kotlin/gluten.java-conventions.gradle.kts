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
 * Convention plugin for Java compilation settings.
 * Configures Java source/target compatibility and compiler options.
 */

plugins {
    java
    `maven-publish`
    checkstyle
}

val javaVersion: String by project

val javaVersionInt = javaVersion.toIntOrNull() ?: 17

java {
    sourceCompatibility = JavaVersion.toVersion(javaVersionInt)
    targetCompatibility = JavaVersion.toVersion(javaVersionInt)

    withSourcesJar()
}

checkstyle {
    toolVersion = "8.29"
    configFile = rootProject.file("dev/checkstyle.xml")
    // Set configDirectory to dev/ so Gradle auto-sets ${config_loc} to the dev/ directory.
    // The checkstyle.xml uses "${config_loc}/checkstyle-suppressions.xml" to locate suppressions.
    // Maven sets config_loc via <propertyExpansion> in pom.xml.
    configDirectory.set(rootProject.file("dev"))
    isIgnoreFailures = false
}

// Exclude generated sources from checkstyle. Maven's checkstyle plugin explicitly limits
// sourceDirectories to src/main/java and src/main/scala. Gradle's checkstyle scans the
// entire source set, which includes protobuf/ANTLR generated files under build/.
tasks.withType<Checkstyle>().configureEach {
    // Only check hand-written sources under src/, not generated code under build/.
    source = source.filter { file ->
        !file.path.contains("/build/")
    }.asFileTree
}

// Exclude logging implementations that leak in via Spark/Hadoop transitive deps.
// Maven does this via global <exclusions> in the parent POM.
configurations.all {
    exclude(group = "org.slf4j", module = "slf4j-log4j12")
    exclude(group = "log4j", module = "log4j")
}

// When the running JDK is newer than targetCompatibility (e.g. JDK 21 with target 17),
// Gradle and its plugins (Java, Scala, etc.) stamp outgoing variants with the running
// JDK version instead of the configured target. This causes variant resolution failures
// between projects (e.g. shims-common@21 vs shims-spark41@17).
// Override TargetJvmVersion on ALL configurations — including legacy ones like "archives"
// and "default", and plugin-added ones like "incrementalScalaAnalysisElements" — so
// inter-project dependencies resolve correctly regardless of the build JDK.
afterEvaluate {
    configurations.configureEach {
        attributes {
            attribute(
                TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE,
                javaVersionInt
            )
        }
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.compilerArgs.addAll(listOf("-Xlint:all,-serial,-path"))
    options.isFork = true
    options.forkOptions.jvmArgs = (options.forkOptions.jvmArgs ?: mutableListOf()).apply {
        add("-Dfile.encoding=UTF-8")
    }
}

tasks.withType<Javadoc>().configureEach {
    options.encoding = "UTF-8"
    isFailOnError = false
}

// Make compileOnly dependencies available for test compilation
// This is needed because Spark dependencies are declared as compileOnly
// but are required for test compilation
configurations {
    testImplementation {
        extendsFrom(configurations.compileOnly.get())
    }
}

// Maven publishing — equivalent to "mvn install"
// Usage: ./gradlew publishToMavenLocal
// Shims modules use a "spark-sql-columnar-" prefix in Maven to match the Maven build.
val mavenArtifactId = if (project.name.startsWith("shims-")) {
    "spark-sql-columnar-${project.name}"
} else {
    project.name
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifactId = mavenArtifactId
        }
    }
}
