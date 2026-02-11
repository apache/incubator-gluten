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

import com.google.protobuf.gradle.*
import java.time.Duration

/**
 * Convention plugin for Protobuf compilation.
 * Configures protobuf-gradle-plugin for generating Java sources from .proto files.
 */

plugins {
    id("com.google.protobuf")
    java
}

val protobufVersion: String by project

dependencies {
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
}

// Fail fast if proto extraction blocks (e.g. due to network or lock issues).
// The extractIncludeProto task scans the entire compile classpath for .proto files,
// which can stall when dependencies cannot be resolved.
tasks.withType<com.google.protobuf.gradle.ProtobufExtract>().configureEach {
    @Suppress("UnstableApiUsage")
    timeout = Duration.ofMinutes(5)
}

// Configure source sets to include generated protobuf sources
sourceSets {
    main {
        java {
            srcDir("build/generated/source/proto/main/java")
        }
    }
    test {
        java {
            srcDir("build/generated/source/proto/test/java")
        }
    }
}
