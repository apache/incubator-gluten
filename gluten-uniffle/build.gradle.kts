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
    id("gluten.spotless")
}

val effectiveSparkPlainVersion: String by rootProject.extra
val uniffleVersion: String by project

// Add Uniffle-specific source directories
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
    test {
        scala {
            srcDir("src-uniffle/test/scala")
            srcDir("src-uniffle/test/java")
            srcDir("src-uniffle-spark$effectiveSparkPlainVersion/test/scala")
            srcDir("src-uniffle-spark$effectiveSparkPlainVersion/test/java")
        }
        resources {
            srcDir("src-uniffle/test/resources")
            srcDir("src-uniffle-spark$effectiveSparkPlainVersion/test/resources")
        }
    }
}

dependencies {
    implementation(project(":gluten-substrait"))

    compileOnly("org.apache.uniffle:rss-client-spark${effectiveSparkPlainVersion.take(1)}-shaded:$uniffleVersion")
}
