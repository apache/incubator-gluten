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
import org.gradle.api.JavaVersion
import org.gradle.api.Project

/**
 * Configures the Spotless extension to match the Maven spotless-maven-plugin configuration
 * in pom.xml. This file is excluded from compilation on JDK < 11 (see build.gradle.kts).
 */
object GlutenSpotless {

    // spotless:off
    private val LICENSE_HEADER = buildString {
        append("/*\n")
        append(" * Licensed to the Apache Software Foundation (ASF) under one or more\n")
        append(" * contributor license agreements.  See the NOTICE file distributed with\n")
        append(" * this work for additional information regarding copyright ownership.\n")
        append(" * The ASF licenses this file to You under the Apache License, Version 2.0\n")
        append(" * (the \"License\"); you may not use this file except in compliance with\n")
        append(" * the License.  You may obtain a copy of the License at\n")
        append(" *\n")
        append(" *    http://www.apache.org/licenses/LICENSE-2.0\n")
        append(" *\n")
        append(" * Unless required by applicable law or agreed to in writing, software\n")
        append(" * distributed under the License is distributed on an \"AS IS\" BASIS,\n")
        append(" * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n")
        append(" * See the License for the specific language governing permissions and\n")
        append(" * limitations under the License.\n")
        append(" */")
    }
    // spotless:on

    fun apply(project: Project) {
        project.pluginManager.apply("com.diffplug.spotless")

        // Match Maven: google-java-format 1.7 for JDK 11, 1.17.0 for JDK 17+.
        // Maven uses <jdk>21</jdk> activation for the java-21 profile which bumps the version.
        // Spotless enforces minimum version 1.10.0 for JDK 17+.
        val runtimeJdk = JavaVersion.current().majorVersion.toInt()
        val googleJavaFormatVersion = if (runtimeJdk >= 17) "1.17.0" else "1.7"

        val spotless = project.extensions.getByType(SpotlessExtension::class.java)
        spotless.java {
            toggleOffOn()
            googleJavaFormat(googleJavaFormatVersion)
            importOrder("org.apache.gluten", "io.substrait.spark", "", "javax", "java", "scala", "\\#")
            removeUnusedImports()
            licenseHeader(LICENSE_HEADER, "package")
            target(
                "src/main/java/**/*.java",
                "src/test/java/**/*.java",
                "src-*/main/java/**/*.java",
                "src-*/test/java/**/*.java",
            )
        }
        if (project.hasProperty("scalaBinaryVersion")) {
            val scalaBinaryVersion = project.property("scalaBinaryVersion") as String
            spotless.scala {
                toggleOffOn()
                scalafmt("3.8.3")
                    .configFile(project.rootProject.file(".scalafmt.conf"))
                    .scalaMajorVersion(scalaBinaryVersion)
                licenseHeader(LICENSE_HEADER, "package")
                target(
                    "src/main/scala/**/*.scala",
                    "src/test/scala/**/*.scala",
                    "src-*/main/scala/**/*.scala",
                    "src-*/test/scala/**/*.scala",
                )
            }
        }
    }
}
