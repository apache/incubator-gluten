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
package io.glutenproject.substrait.rel;

import java.util.List;
import java.util.Map;

public class IcebergLocalFilesNode extends LocalFilesNode {

  class DeleteFile {
    private final String path;
    private final Integer fileContent;
    private final ReadFileFormat fileFormat;
    private final Long fileSize;
    private final Long recordCount;
    private final Map<Integer, String> lowerBounds;
    private final Map<Integer, String> upperBounds;

    DeleteFile(
        String path,
        Integer fileContent,
        ReadFileFormat fileFormat,
        Long fileSize,
        Long recordCount,
        Map<Integer, String> lowerBounds,
        Map<Integer, String> upperBounds) {
      this.path = path;
      this.fileContent = fileContent;
      this.fileFormat = fileFormat;
      this.fileSize = fileSize;
      this.recordCount = recordCount;
      this.lowerBounds = lowerBounds;
      this.upperBounds = upperBounds;
    }
  }

  // TODO: Add delete file support for MOR iceberg table

  IcebergLocalFilesNode(
      Integer index,
      List<String> paths,
      List<Long> starts,
      List<Long> lengths,
      List<Map<String, String>> partitionColumns,
      ReadFileFormat fileFormat,
      List<String> preferredLocations) {
    super(index, paths, starts, lengths, partitionColumns, fileFormat, preferredLocations);
  }
}
