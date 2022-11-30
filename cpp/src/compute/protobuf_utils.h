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

#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <google/protobuf/message.h>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

// Common for both projector and filters.
bool ParseProtobuf(const uint8_t* buf, int bufLen, google::protobuf::Message* msg);

arrow::Result<std::shared_ptr<arrow::Buffer>> SubstraitFromJSON(
    arrow::util::string_view type_name,
    arrow::util::string_view json);

arrow::Result<std::string> SubstraitToJSON(arrow::util::string_view type_name, const arrow::Buffer& buf);

// Write a Protobuf message into a specified file with JSON format.
void MessageToJSONFile(const google::protobuf::Message& message, const std::string& file_path);
