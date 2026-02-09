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

import com.diffplug.gradle.spotless.SpotlessExtension

/**
 * Convention plugin for code formatting using Spotless.
 * Configures scalafmt for Scala and google-java-format for Java.
 */

plugins {
    id("com.diffplug.spotless")
}

val scalaBinaryVersion: String by project

configure<SpotlessExtension> {
    encoding("UTF-8")

    val licenseHeader = """
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
    """.trimIndent()

    java {
        target(
            "src/main/java/**/*.java",
            "src/test/java/**/*.java",
            "src-*/main/java/**/*.java",
            "src-*/test/java/**/*.java"
        )
        toggleOffOn()
        googleJavaFormat("1.17.0")
        importOrder("org.apache.gluten", "io.substrait.spark", "", "javax", "java", "scala", "\\#")
        removeUnusedImports()
        licenseHeader(licenseHeader, "package")
    }

    scala {
        target(
            "src/main/scala/**/*.scala",
            "src/test/scala/**/*.scala",
            "src-*/main/scala/**/*.scala",
            "src-*/test/scala/**/*.scala"
        )
        toggleOffOn()
        scalafmt("3.8.3").configFile(rootProject.file(".scalafmt.conf")).scalaMajorVersion(scalaBinaryVersion)
        licenseHeader(licenseHeader, "package")
    }

    kotlinGradle {
        target("*.gradle.kts")
        ktlint()
    }
}
