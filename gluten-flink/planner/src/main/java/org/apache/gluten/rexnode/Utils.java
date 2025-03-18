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
package org.apache.gluten.rexnode;

import io.github.zhztheplayer.velox4j.serializable.ISerializableRegistry;
import io.github.zhztheplayer.velox4j.variant.VariantRegistry;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/** Utility to store some useful functions. */
public class Utils {

    private static boolean registryInitialized = false;

    // Get names for project node.
    public static List<String> getNamesFromRowType(LogicalType logicalType) {
        if (logicalType instanceof RowType) {
            RowType rowType = (RowType) logicalType;
            return rowType.getFieldNames();
        } else {
            throw new RuntimeException("Output type is not row type: " + logicalType);
        }
    }

    // Init serialize related registries.
    public static void registerRegistry() {
        if (!registryInitialized) {
            registryInitialized = true;
            VariantRegistry.registerAll();
            ISerializableRegistry.registerAll();
        }
    }
}
