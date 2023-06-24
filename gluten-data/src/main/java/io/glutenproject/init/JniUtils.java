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

package io.glutenproject.init;

import java.util.Map;

import com.google.protobuf.Any;
import io.glutenproject.substrait.expression.ExpressionBuilder;
import io.glutenproject.substrait.expression.StringMapNode;
import io.glutenproject.substrait.extensions.AdvancedExtensionNode;
import io.glutenproject.substrait.extensions.ExtensionBuilder;
import io.glutenproject.substrait.plan.PlanBuilder;

public class JniUtils {

    public static byte[] toNativeConf(Map<String, String> confs) {
        StringMapNode stringMapNode = ExpressionBuilder.makeStringMap(confs);
        AdvancedExtensionNode extensionNode = ExtensionBuilder
                .makeAdvancedExtension(Any.pack(stringMapNode.toProtobuf()));
        return PlanBuilder.makePlan(extensionNode).toProtobuf().toByteArray();
    }
}
