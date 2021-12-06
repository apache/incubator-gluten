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

package com.intel.oap.substrait.rel;

import io.substrait.ReadRel;

import java.io.Serializable;
import java.util.ArrayList;

public class LocalFilesNode implements Serializable {
    private final Integer index;
    private final ArrayList<String> paths = new ArrayList<>();
    private final ArrayList<Long> starts = new ArrayList<>();
    private final ArrayList<Long> lengths = new ArrayList<>();

    LocalFilesNode(Integer index, ArrayList<String> paths,
                   ArrayList<Long> starts, ArrayList<Long> lengths) {
        this.index = index;
        this.paths.addAll(paths);
        this.starts.addAll(starts);
        this.lengths.addAll(lengths);
    }

    public ReadRel.LocalFiles toProtobuf() {
        ReadRel.LocalFiles.Builder localFilesBuilder = ReadRel.LocalFiles.newBuilder();
        localFilesBuilder.setIndex(index.intValue());
        for (int i = 0; i < paths.size(); i++) {
            ReadRel.LocalFiles.FileOrFiles.Builder fileBuilder =
                    ReadRel.LocalFiles.FileOrFiles.newBuilder();
            fileBuilder.setUriPath(paths.get(i));
            fileBuilder.setLength(lengths.get(i));
            fileBuilder.setStart(starts.get(i));
            localFilesBuilder.addItems(fileBuilder.build());
        }
        return localFilesBuilder.build();
    }
}
