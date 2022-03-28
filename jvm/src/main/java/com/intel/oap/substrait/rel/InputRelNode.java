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

import com.intel.oap.substrait.type.TypeNode;
import io.substrait.proto.InputRel;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Rel;
import io.substrait.proto.Type;

import java.io.Serializable;
import java.util.ArrayList;

public class InputRelNode implements RelNode, Serializable {
    private final ArrayList<String> names = new ArrayList<>();
    private final ArrayList<TypeNode> types = new ArrayList<>();
    private final Long iterIdx;

    InputRelNode(ArrayList<String> names, ArrayList<TypeNode> types,
                 Long iterIdx) {
        this.names.addAll(names);
        this.types.addAll(types);
        this.iterIdx = iterIdx;
    }

    public Rel toProtobuf() {
        Type.Struct.Builder structBuilder = Type.Struct.newBuilder();
        for (TypeNode typeNode : types) {
            structBuilder.addTypes(typeNode.toProtobuf());
        }
        NamedStruct.Builder nStructBuilder = NamedStruct.newBuilder();
        nStructBuilder.setStruct(structBuilder.build());
        for (String name : names) {
            nStructBuilder.addNames(name);
        }
        InputRel.Builder inputBuilder = InputRel.newBuilder();
        inputBuilder.setInputSchema(nStructBuilder.build());
        inputBuilder.setIterIdx(iterIdx);
        Rel.Builder builder = Rel.newBuilder();
        builder.setInput(inputBuilder.build());
        return builder.build();
    }
}
