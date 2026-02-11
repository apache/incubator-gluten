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
 * Convention plugin for code formatting via Spotless.
 *
 * Skipped when the build JDK is below 11 because spotless-plugin-gradle requires Java 11+.
 * For Java 11+ it configures:
 *   - Java: google-java-format, import ordering, license headers
 *   - Scala: scalafmt with .scalafmt.conf, license headers
 *
 * This mirrors the Maven spotless-maven-plugin configuration in pom.xml.
 */

// No-op when JDK < 11.  The actual configuration is in GlutenSpotless.kt,
// which is only compiled when spotless-plugin-gradle is on the classpath (JDK 11+).
// We use reflection to avoid a compile-time dependency on the GlutenSpotless class.
if (JavaVersion.current().isJava11Compatible) {
    val clazz = Class.forName("GlutenSpotless")
    val method = clazz.getMethod("apply", org.gradle.api.Project::class.java)
    method.invoke(clazz.kotlin.objectInstance, project)
}
