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
 * Convention plugin for Scala library modules.
 * Adds version-specific source sets based on Spark version.
 */

plugins {
    id("gluten.scala-conventions")
    `java-library` // Provides 'api' configuration for transitive dependencies
}

val sparkVersion: String by project
val sparkPlainVersion = when (sparkVersion) {
    "3.3" -> "33"
    "3.4" -> "34"
    "3.5" -> "35"
    "4.0" -> "40"
    "4.1" -> "41"
    else -> "41"
}

// Add Spark version-specific source directories if they exist
sourceSets {
    main {
        scala {
            srcDir("src-spark${sparkPlainVersion}/main/scala")
            srcDir("src-spark${sparkPlainVersion}/main/java")
        }
        resources {
            srcDir("src-spark${sparkPlainVersion}/main/resources")
        }
    }
    test {
        scala {
            srcDir("src-spark${sparkPlainVersion}/test/scala")
            srcDir("src-spark${sparkPlainVersion}/test/java")
        }
        resources {
            srcDir("src-spark${sparkPlainVersion}/test/resources")
        }
    }
}

// Create test JAR
val testJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets.test.get().output)
}

artifacts {
    add("archives", testJar)
}

configurations {
    create("testArtifacts") {
        extendsFrom(configurations.testRuntimeClasspath.get())
    }
}

artifacts {
    add("testArtifacts", testJar)
}
