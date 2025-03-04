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

cd `dirname $0`

# Check if clang-format-15 is installed
if ! command -v clang-format-15 &> /dev/null
then
    echo "clang-format-15 could not be found"
    echo "Installing clang-format-15..."
    sudo apt update
    sudo apt install clang-format-15
fi

find ../cpp/core -regex '.*\.\(cc\|hpp\|cu\|c\|h\)' -exec clang-format-15 -style=file -i {} \;
find ../cpp/velox -regex '.*\.\(cc\|hpp\|cu\|c\|h\)' -exec clang-format-15 -style=file -i {} \;