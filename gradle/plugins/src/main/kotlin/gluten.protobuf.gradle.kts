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

// The protobuf plugin's extractInclude*Proto tasks scan the entire compile classpath
// for .proto files (via the compileProtoPath configuration that extends compileOnly +
// implementation). In Gluten this means scanning 600+ transitive Spark/Hadoop JARs,
// none of which contain .proto files. This causes CI stalls when dependency resolution
// blocks on network or lock contention.
//
// Fix: remove the extendsFrom relationships on compileProtoPath / testCompileProtoPath
// so only explicitly declared protobuf dependencies are scanned.
afterEvaluate {
    listOf("compileProtoPath", "testCompileProtoPath").forEach { name ->
        configurations.findByName(name)?.let { protoPath ->
            protoPath.setExtendsFrom(emptyList())
            // Re-add only protobuf-java so its bundled .proto files are available as imports.
            protoPath.dependencies.add(
                project.dependencies.create("com.google.protobuf:protobuf-java:$protobufVersion")
            )
        }
    }
}

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
