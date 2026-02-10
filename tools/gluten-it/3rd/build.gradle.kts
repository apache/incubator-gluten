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
    id("gluten.java-conventions")
    id("com.gradleup.shadow")
}

dependencies {
    implementation("io.trino.tpcds:tpcds:1.4")
    implementation("io.trino.tpch:tpch:1.1")
}

tasks.shadowJar {
    archiveClassifier.set("")
    relocate("com.google.common", "org.apache.gluten.integration.shaded.com.google.common")

    // Exclude signatures
    exclude("META-INF/*.SF")
    exclude("META-INF/*.DSA")
    exclude("META-INF/*.RSA")
}

// Use the shadow JAR as the module's primary artifact
tasks.jar { enabled = false }
configurations {
    runtimeElements { outgoing { artifact(tasks.shadowJar) } }
    apiElements { outgoing { artifact(tasks.shadowJar) } }
}
