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

import io.substrait.proto.ReadRel;

import java.io.Serializable;
import java.util.ArrayList;

public class LocalFilesNode implements Serializable {
    private final Integer index;
    private final ArrayList<String> paths = new ArrayList<>();
    private final ArrayList<Long> starts = new ArrayList<>();
    private final ArrayList<Long> lengths = new ArrayList<>();
    private Boolean iterAsInput = false;

    LocalFilesNode(Integer index, ArrayList<String> paths,
                   ArrayList<Long> starts, ArrayList<Long> lengths) {
        this.index = index;
        this.paths.addAll(paths);
        this.starts.addAll(starts);
        this.lengths.addAll(lengths);
    }

    LocalFilesNode(String iterPath) {
        this.index = null;
        this.paths.add(iterPath);
        this.iterAsInput = true;
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
            // TODO: Support multiple file format
            fileBuilder.setFormat(ReadRel.LocalFiles.FileOrFiles.FileFormat.FILE_FORMAT_PARQUET);
            localFilesBuilder.addItems(fileBuilder.build());
        }
        return localFilesBuilder.build();
    }
}
