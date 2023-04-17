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

#include "shuffle/PartitionWriter.h"
#include "shuffle/LocalPartitionWriter.h"
#include "shuffle/rss/RemotePartitionWriter.h"

namespace gluten {

arrow::Result<std::shared_ptr<ShuffleWriter::PartitionWriter>> ShuffleWriter::PartitionWriter::Make(
    ShuffleWriter* shuffle_writer,
    int32_t num_partitions) {
  const std::string& partition_writer_type = shuffle_writer->Options().partition_writer_type;
  if (partition_writer_type == "local") {
    return LocalPartitionWriter::Create(shuffle_writer, num_partitions);
  } else {
    return RemotePartitionWriter::Make(shuffle_writer, num_partitions);
  }
}

} // namespace gluten
