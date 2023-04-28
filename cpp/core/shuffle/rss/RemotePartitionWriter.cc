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

#include "shuffle/rss/RemotePartitionWriter.h"
#include "shuffle/rss/CelebornPartitionWriter.h"

namespace gluten {

arrow::Result<std::shared_ptr<RemotePartitionWriter>> RemotePartitionWriter::Make(
    ShuffleWriter* shuffle_writer,
    int32_t num_partitions) {
  const std::string& partition_writer_type = shuffle_writer->Options().partition_writer_type;
  if (partition_writer_type == "celeborn") {
    return CelebornPartitionWriter::Create(shuffle_writer, num_partitions);
  }
  return arrow::Status::NotImplemented("Partition Writer Type " + partition_writer_type + " not supported yet.");
}

} // namespace gluten
