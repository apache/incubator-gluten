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

import java.util.HashMap;
import java.util.Map;

/** Mapping of flink function and velox function. */
public class FunctionMappings {
    // A map stores the relationship between flink function name and velox function.
    private static Map<String, String> functionMappings = new HashMap() {
        {
            // TODO: support more functions.
            put(">", "greaterthan");
            put("<", "lessthan");
            put("=", "equalto");
            put("CAST", "cast");
            put("CASE", "case");
            put("*", "multiply");
        }
    };

    public static String toVeloxFunction(String funcName) {
        if (functionMappings.containsKey(funcName)) {
            return functionMappings.get(funcName);
        } else {
            throw new RuntimeException("Function not supported: " + funcName);
        }
    }
}
