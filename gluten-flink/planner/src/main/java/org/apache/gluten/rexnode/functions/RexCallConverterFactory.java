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
package org.apache.gluten.rexnode.functions;

import java.util.Map;

public class RexCallConverterFactory {

  // Better to new Converter for each call. Reusing an object can easily introduce potential
  // issues.
  private static Map<String, RexCallConverterBuilder> converters =
      Map.of(
          ">", () -> new AlignInputsTypeRexCallConverter("greaterthan"),
          "<", () -> new AlignInputsTypeRexCallConverter("lessthan"),
          "=", () -> new AlignInputsTypeRexCallConverter("equalto"),
          "*", () -> new AlignInputsTypeRexCallConverter("multiply"),
          "-", () -> new AlignInputsTypeRexCallConverter("subtract"),
          "+", () -> new AlignInputsTypeRexCallConverter("add"),
          "MOD", () -> new ModRexCallConverter(),
          "CAST", () -> new DefaultRexCallConverter("cast"),
          "CASE", () -> new DefaultRexCallConverter("if"),
          "AND", () -> new DefaultRexCallConverter("and"));

  public static RexCallConverter getConverter(String operatorName) {
    RexCallConverterBuilder builder = converters.get(operatorName);
    if (builder == null) {
      throw new RuntimeException("Function not supported: " + operatorName);
    }
    return builder.build();
  }

  private interface RexCallConverterBuilder {
    RexCallConverter build();
  }
}
