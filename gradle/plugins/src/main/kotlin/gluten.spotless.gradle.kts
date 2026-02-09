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
 * Convention plugin for code formatting.
 *
 * Spotless formatting is handled by the Maven spotless-maven-plugin (2.27.2),
 * which supports Java 8+ and is already configured in each module's pom.xml.
 * Run formatting via: ./build/mvn spotless:apply -N
 *
 * This Gradle plugin is intentionally a no-op to avoid pulling in
 * spotless-plugin-gradle which requires Java 11+.
 */
