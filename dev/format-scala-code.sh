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

# If a new profile is introduced for new modules, please add it here to ensure
# the new modules are covered.
PROFILES="-Pbackends-velox -Pceleborn,uniffle -Piceberg,delta,hudi,paimon \
          -Pspark-3.2,spark-3.3,spark-3.4,spark-3.5,spark-4.0 -Pspark-ut"

COMMAND=$1

if [[ "$COMMAND" == "check" ]]; then
  echo "Checking Scala code style.."
  mvn -q spotless:check $PROFILES
elif [[ "$COMMAND" == "apply" ]] || [[ "$COMMAND" == "" ]]; then
  echo "Fixing Scala code style.."
  mvn -q spotless:apply $PROFILES
else
  echo "Unrecognized option."
  exit 1
fi
