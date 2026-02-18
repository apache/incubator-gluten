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

import org.apache.gluten.config.GlutenConfig;

import io.substrait.proto.ReadRel;
import io.substrait.proto.ReadRel.LocalFiles.FileOrFiles.OrcReadOptions;
import io.substrait.proto.ReadRel.LocalFiles.FileOrFiles.PaimonReadOptions;
import io.substrait.proto.ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PaimonLocalFilesNode extends LocalFilesNode {
  private final List<Integer> buckets;
  private final List<Long> firstRowIds;
  private final List<Long> maxSequenceNumbers;
  private final List<Integer> splitGroups;
  private final boolean useHiveSplit;
  private final List<String> primaryKeys;
  private final boolean allRawConvertible;

  public PaimonLocalFilesNode(
      Integer index,
      List<String> paths,
      List<Long> starts,
      List<Long> lengths,
      List<Map<String, String>> partitionColumns,
      ReadFileFormat fileFormat,
      List<String> preferredLocations,
      Map<String, String> properties,
      List<Integer> buckets,
      List<Long> firstRowIds,
      List<Long> maxSequenceNumbers,
      List<Integer> splitGroups,
      boolean useHiveSplit,
      List<String> primaryKeys,
      boolean allRawConvertible) {
    super(
        index,
        paths,
        starts,
        lengths,
        new ArrayList<>(),
        new ArrayList<>(),
        partitionColumns,
        new ArrayList<>(),
        fileFormat,
        preferredLocations,
        properties,
        new ArrayList<>());
    this.buckets = buckets;
    this.firstRowIds = firstRowIds;
    this.maxSequenceNumbers = maxSequenceNumbers;
    this.splitGroups = splitGroups;
    this.useHiveSplit = useHiveSplit;
    this.primaryKeys = primaryKeys;
    this.allRawConvertible = allRawConvertible;
  }

  @Override
  protected void processFileBuilder(ReadRel.LocalFiles.FileOrFiles.Builder fileBuilder, int index) {
    Integer bucket = buckets.get(index);
    Long firstRowId = firstRowIds.get(index);
    Long maxSequenceNumber = maxSequenceNumbers.get(index);
    Integer splitGroup = splitGroups.get(index);
    PaimonReadOptions.Builder paimonBuilder = PaimonReadOptions.newBuilder();
    paimonBuilder.setBucket(bucket);
    paimonBuilder.setFirstRowId(firstRowId);
    paimonBuilder.setMaxSequenceNumber(maxSequenceNumber);
    paimonBuilder.setSplitGroup(splitGroup);
    paimonBuilder.setUseHiveSplit(useHiveSplit);
    paimonBuilder.addAllPrimaryKeys(primaryKeys);
    paimonBuilder.setRawConvertible(allRawConvertible);

    switch (fileFormat) {
      case ParquetReadFormat:
        ParquetReadOptions parquetReadOptions =
            ParquetReadOptions.newBuilder()
                .setEnableRowGroupMaxminIndex(GlutenConfig.get().enableParquetRowGroupMaxMinIndex())
                .build();
        paimonBuilder.setParquet(parquetReadOptions);
        break;
      case OrcReadFormat:
        paimonBuilder.setOrc(OrcReadOptions.newBuilder().build());
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported file format " + fileFormat.name() + " for paimon data file.");
    }
    fileBuilder.setPaimon(paimonBuilder);
  }
}
