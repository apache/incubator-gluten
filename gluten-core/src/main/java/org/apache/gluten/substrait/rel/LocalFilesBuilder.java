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
package org.apache.gluten.substrait.rel;

import java.util.List;
import java.util.Map;

public class LocalFilesBuilder {
  private LocalFilesBuilder() {}

  public static LocalFilesNode makeLocalFiles(
      Integer index,
      List<String> paths,
      List<Long> starts,
      List<Long> lengths,
      List<Map<String, String>> partitionColumns,
      List<Map<String, String>> metadataColumns,
      LocalFilesNode.ReadFileFormat fileFormat,
      List<String> preferredLocations) {
    return new LocalFilesNode(
        index,
        paths,
        starts,
        lengths,
        partitionColumns,
        metadataColumns,
        fileFormat,
        preferredLocations);
  }

  public static LocalFilesNode makeLocalFiles(String iterPath) {
    return new LocalFilesNode(iterPath);
  }
}
