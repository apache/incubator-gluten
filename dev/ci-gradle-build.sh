#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Translates Maven-style profile/property arguments to Gradle properties and runs a Gradle build.
# This allows CI workflows to use the same arguments for both Maven and Gradle builds.
#
# Usage (build only):
#   bash dev/ci-gradle-build.sh -Pspark-3.5 -Pjava-8 -Pbackends-velox -DskipTests
#
# Usage (build + test with tag filtering):
#   bash dev/ci-gradle-build.sh -Pspark-3.5 -Pjava-17 -Pbackends-velox -Pspark-ut \
#     -DtagsToExclude=org.apache.gluten.tags.UDFTest,org.apache.gluten.tags.SkipTest \
#     -DargLine="-Dspark.test.home=/opt/shims/spark35/spark_home/"

set -euo pipefail

GRADLE_ARGS=""
SKIP_TESTS=""
GRADLE_TASK="build"
EXTRA_JVM_ARGS=""

for arg in "$@"; do
  case "$arg" in
    # Spark/Java/Scala version profiles
    -Pspark-*)    GRADLE_ARGS="$GRADLE_ARGS -PsparkVersion=${arg#-Pspark-}" ;;
    -Pjava-*)     GRADLE_ARGS="$GRADLE_ARGS -PjavaVersion=${arg#-Pjava-}" ;;
    -Pscala-*)    GRADLE_ARGS="$GRADLE_ARGS -PscalaBinaryVersion=${arg#-Pscala-}" ;;
    -Pbackends-*) GRADLE_ARGS="$GRADLE_ARGS -Pbackend=${arg#-Pbackends-}" ;;
    # Hadoop version profile
    -Phadoop-*)   GRADLE_ARGS="$GRADLE_ARGS -PhadoopVersion=${arg#-Phadoop-}" ;;
    # Optional modules
    -Pdelta)      GRADLE_ARGS="$GRADLE_ARGS -Pdelta=true" ;;
    -Piceberg)    GRADLE_ARGS="$GRADLE_ARGS -Piceberg=true" ;;
    -Phudi)       GRADLE_ARGS="$GRADLE_ARGS -Phudi=true" ;;
    -Ppaimon)     GRADLE_ARGS="$GRADLE_ARGS -Ppaimon=true" ;;
    -Pceleborn)   GRADLE_ARGS="$GRADLE_ARGS -Pceleborn=true" ;;
    -Pceleborn-0.5) GRADLE_ARGS="$GRADLE_ARGS -Pceleborn=true -PcelebornVersion=0.5.4" ;;
    -Pceleborn-0.6) GRADLE_ARGS="$GRADLE_ARGS -Pceleborn=true -PcelebornVersion=0.6.1" ;;
    -Puniffle)    GRADLE_ARGS="$GRADLE_ARGS -Puniffle=true" ;;
    -Pkafka)      GRADLE_ARGS="$GRADLE_ARGS -Pkafka=true" ;;
    -PglutenIt*)  GRADLE_ARGS="$GRADLE_ARGS $arg" ;;
    # Test control
    -DskipTests)  SKIP_TESTS="-x test" ;;
    # ScalaTest tag filtering
    -DtagsToInclude=*) GRADLE_ARGS="$GRADLE_ARGS -PtagsToInclude=${arg#-DtagsToInclude=}" ;;
    -DtagsToExclude=*) GRADLE_ARGS="$GRADLE_ARGS -PtagsToExclude=${arg#-DtagsToExclude=}" ;;
    # Spark test home and extra JVM args (extracted from -DargLine)
    -DargLine=*)
      ARGLINE="${arg#-DargLine=}"
      # Extract spark.test.home from argLine
      if [[ "$ARGLINE" =~ -Dspark\.test\.home=([^ \"]+) ]]; then
        GRADLE_ARGS="$GRADLE_ARGS -PsparkTestHome=${BASH_REMATCH[1]}"
      fi
      # Collect other -D system properties for test JVM
      for prop in $ARGLINE; do
        case "$prop" in
          -Dspark.test.home=*) ;; # already handled above
          -D*)
            if [ -z "$EXTRA_JVM_ARGS" ]; then
              EXTRA_JVM_ARGS="$prop"
            else
              EXTRA_JVM_ARGS="$EXTRA_JVM_ARGS,$prop"
            fi
            ;;
        esac
      done
      ;;
    # Ignore Maven-specific flags that have no Gradle equivalent
    -Pspark-ut|-Piceberg-test|-ntp|-pl|-am) ;;
    -Dmaven.*|-Dtest=*|-DfailIfNoTests=*|-Dexec.skip) ;;
    *)
      echo "Warning: ignoring unknown argument: $arg" >&2
      ;;
  esac
done

# Pass extra JVM args if any were collected from -DargLine
if [ -n "$EXTRA_JVM_ARGS" ]; then
  GRADLE_ARGS="$GRADLE_ARGS -PtestJvmArgs=$EXTRA_JVM_ARGS"
fi

echo "Gradle command: ./gradlew $GRADLE_TASK $SKIP_TESTS $GRADLE_ARGS --no-daemon"

# Pre-download Gradle distribution with retry to handle transient network failures.
# This only retries the download, not the actual build/test execution.
MAX_RETRIES=3
RETRY_DELAY=10
for i in $(seq 1 $MAX_RETRIES); do
  if ./gradlew --version --no-daemon > /dev/null 2>&1; then
    break
  fi
  if [ "$i" -eq "$MAX_RETRIES" ]; then
    echo "Failed to download Gradle distribution after $MAX_RETRIES attempts" >&2
    exit 1
  fi
  echo "Gradle distribution download failed (attempt $i/$MAX_RETRIES), retrying in ${RETRY_DELAY}s..." >&2
  sleep $RETRY_DELAY
done

# shellcheck disable=SC2086
./gradlew $GRADLE_TASK $SKIP_TESTS $GRADLE_ARGS --no-daemon
