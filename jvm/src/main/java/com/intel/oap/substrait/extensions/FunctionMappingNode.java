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

package com.intel.oap.substrait.extensions;

import io.substrait.*;

import java.io.Serializable;

public class FunctionMappingNode implements MappingNode, Serializable {
    private final String name;
    private final Long functionId;

    public FunctionMappingNode(String name, Long functionId) {
        this.name = name;
        this.functionId = functionId;
    }

    @Override
    public Extensions.Mapping toProtobuf() {
        Extensions.FunctionId.Builder funcIdBuilder =
                Extensions.FunctionId.newBuilder();
        funcIdBuilder.setId(functionId);

        Extensions.Mapping.FunctionMapping.Builder funcBuilder =
                Extensions.Mapping.FunctionMapping.newBuilder();
        funcBuilder.setName(name);
        funcBuilder.setFunctionId(funcIdBuilder.build());

        Extensions.Mapping.Builder builder =
                Extensions.Mapping.newBuilder();
        builder.setFunctionMapping(funcBuilder.build());
        return builder.build();
    }
}
