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

import io.glutenproject.GlutenConfig;
import io.glutenproject.expression.ConverterUtils;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Type;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

public class LocalFilesNode implements Serializable {
  private final Integer index;
  private final ArrayList<String> paths = new ArrayList<>();
  private final ArrayList<Long> starts = new ArrayList<>();
  private final ArrayList<Long> lengths = new ArrayList<>();

  // The format of file to read.
  public enum ReadFileFormat {
    ParquetReadFormat(),
    ArrowReadFormat(),
    OrcReadFormat(),
    DwrfReadFormat(),
    MergeTreeReadFormat(),
    TextReadFormat(),
    JsonReadFormat(),
    UnknownFormat()
  }

  private ReadFileFormat fileFormat = ReadFileFormat.UnknownFormat;
  private Boolean iterAsInput = false;
  private StructType fileSchema;
  private Map<String, String> fileReadProperties;

  LocalFilesNode(Integer index, ArrayList<String> paths,
                 ArrayList<Long> starts, ArrayList<Long> lengths,
                 ReadFileFormat fileFormat) {
    this.index = index;
    this.paths.addAll(paths);
    this.starts.addAll(starts);
    this.lengths.addAll(lengths);
    this.fileFormat = fileFormat;
  }

  LocalFilesNode(String iterPath) {
    this.index = null;
    this.paths.add(iterPath);
    this.iterAsInput = true;
  }

  public void setFileSchema(StructType schema) {
    this.fileSchema = schema;
  }

  public void setFileReadProperties(Map<String, String> fileReadProperties) {
    this.fileReadProperties = fileReadProperties;
  }

  public ReadRel.LocalFiles toProtobuf() {
    ReadRel.LocalFiles.Builder localFilesBuilder = ReadRel.LocalFiles.newBuilder();
    // The input is iterator, and the path is in the format of: Iterator:index.
    if (iterAsInput && paths.size() > 0) {
      ReadRel.LocalFiles.FileOrFiles.Builder fileBuilder =
          ReadRel.LocalFiles.FileOrFiles.newBuilder();
      fileBuilder.setUriFile(paths.get(0));
      localFilesBuilder.addItems(fileBuilder.build());
      return localFilesBuilder.build();
    }
    if (paths.size() != starts.size() || paths.size() != lengths.size()) {
      throw new RuntimeException("Invalid parameters.");
    }
    for (int i = 0; i < paths.size(); i++) {
      ReadRel.LocalFiles.FileOrFiles.Builder fileBuilder =
          ReadRel.LocalFiles.FileOrFiles.newBuilder();
      fileBuilder.setUriFile(paths.get(i));
      if (index != null) {
        fileBuilder.setPartitionIndex(index);
      }
      fileBuilder.setLength(lengths.get(i));
      fileBuilder.setStart(starts.get(i));
      switch (fileFormat) {
        case ParquetReadFormat:
          ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions parquetReadOptions =
              ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions.newBuilder().build();
          fileBuilder.setParquet(parquetReadOptions);
          break;
        case OrcReadFormat:
          ReadRel.LocalFiles.FileOrFiles.OrcReadOptions orcReadOptions =
              ReadRel.LocalFiles.FileOrFiles.OrcReadOptions.newBuilder().build();
          fileBuilder.setOrc(orcReadOptions);
          break;
        case DwrfReadFormat:
          ReadRel.LocalFiles.FileOrFiles.DwrfReadOptions dwrfReadOptions =
              ReadRel.LocalFiles.FileOrFiles.DwrfReadOptions.newBuilder().build();
          fileBuilder.setDwrf(dwrfReadOptions);
          break;
        case TextReadFormat:
          String fieldDelimiter =
                  fileReadProperties.getOrDefault("field.delim", new String(new char[]{0x01}));
          NamedStruct.Builder nStructBuilder = NamedStruct.newBuilder();
          if (fileSchema != null) {
            Type.Struct.Builder structBuilder = Type.Struct.newBuilder();
            nStructBuilder.setStruct(structBuilder.build());
            for (StructField field : fileSchema.fields()) {
              structBuilder.addTypes(
                      ConverterUtils.getTypeNode(field.dataType(), field.nullable()).toProtobuf());
              nStructBuilder.addNames(field.name());
            }
          }
          ReadRel.LocalFiles.FileOrFiles.TextReadOptions textReadOptions =
                  ReadRel.LocalFiles.FileOrFiles.TextReadOptions.newBuilder()
                          .setFieldDelimiter(fieldDelimiter)
                          .setMaxBlockSize(GlutenConfig.getConf().getInputRowMaxBlockSize())
                          .setSchema(nStructBuilder.build())
                          .build();
          fileBuilder.setText(textReadOptions);
          break;
        case JsonReadFormat:
          ReadRel.LocalFiles.FileOrFiles.JsonReadOptions jsonReadOptions =
                  ReadRel.LocalFiles.FileOrFiles.JsonReadOptions.newBuilder()
                          .setMaxBlockSize(GlutenConfig.getConf().getInputRowMaxBlockSize())
                          .build();
          fileBuilder.setJson(jsonReadOptions);
          break;
        default:
          break;
      }
      localFilesBuilder.addItems(fileBuilder.build());
    }
    return localFilesBuilder.build();
  }
}
