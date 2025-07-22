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

import org.apache.gluten.rexnode.RexConversionContext;
import org.apache.gluten.rexnode.ValidationResult;

import org.apache.calcite.rex.RexCall;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RexCallConverterFactory {

  /**
   * Better to new Converter for each call. Reusing an object can easily introduce potential issues.
   *
   * <p>A single operator (e.g., '+') may map to multiple converters, such as arithmetic addition
   * and timestamp interval addition, which require distinct implementations. We need to find the
   * only suitable converter for the given RexCall node.
   */
  private static Map<String, List<RexCallConverterBuilder>> converters =
      Map.ofEntries(
          Map.entry(
              ">",
              Arrays.asList(
                  () -> new BasicArithmeticOperatorRexCallConverter("greaterthan"),
                  () -> new StringCompareRexCallConverter("greaterthan"),
                  () -> new StringNumberCompareRexCallConverter("greaterthan"))),
          Map.entry(
              "<",
              Arrays.asList(
                  () -> new BasicArithmeticOperatorRexCallConverter("lessthan"),
                  () -> new StringCompareRexCallConverter("lessthan"),
                  () -> new StringNumberCompareRexCallConverter("lessthan"))),
          Map.entry(
              "=",
              Arrays.asList(
                  () -> new BasicArithmeticOperatorRexCallConverter("equalto"),
                  () -> new StringCompareRexCallConverter("equalto"),
                  () -> new StringNumberCompareRexCallConverter("equalto"))),
          Map.entry(
              "*", Arrays.asList(() -> new BasicArithmeticOperatorRexCallConverter("multiply"))),
          Map.entry("-", Arrays.asList(() -> new SubtractRexCallConverter())),
          Map.entry("+", Arrays.asList(() -> new BasicArithmeticOperatorRexCallConverter("add"))),
          Map.entry("MOD", Arrays.asList(() -> new ModRexCallConverter())),
          Map.entry("CAST", Arrays.asList(() -> new DefaultRexCallConverter("cast"))),
          Map.entry("CASE", Arrays.asList(() -> new DefaultRexCallConverter("if"))),
          Map.entry("AND", Arrays.asList(() -> new DefaultRexCallConverter("and"))),
          Map.entry("SEARCH", Arrays.asList(() -> new DefaultRexCallConverter("in"))));

  public static RexCallConverter getConverter(RexCall callNode, RexConversionContext context) {
    String operatorName = callNode.getOperator().getName();
    List<RexCallConverterBuilder> builders = converters.get(operatorName);
    if (builders == null) {
      throw new RuntimeException("Function not supported: " + operatorName);
    }

    List<String> failureMessages = new ArrayList<>();
    List<RexCallConverter> converterList =
        builders.stream()
            .map(RexCallConverterBuilder::build)
            .filter(
                c -> {
                  ValidationResult validationResult = c.isSuitable(callNode, context);
                  if (!validationResult.isOk()) {
                    failureMessages.add(
                        c.getClass().getName() + ": " + validationResult.getMessage());
                    return false;
                  } else {
                    return true;
                  }
                })
            .collect(Collectors.toList());

    if (converterList.size() > 1) {
      String converterClasses =
          converterList.stream()
              .map(converter -> converter.getClass().getName())
              .collect(Collectors.joining(", "));
      String message =
          String.format(
              "Multiple converters found for: %s. Converters: %s.", operatorName, converterClasses);
      throw new RuntimeException(message);
    } else if (converterList.isEmpty()) {
      String message =
          String.format(
              "No suitable converter found for: %s. Reason:\n%s",
              operatorName, String.join("\n", failureMessages));
      throw new RuntimeException(message);
    }

    return converterList.get(0);
  }

  private interface RexCallConverterBuilder {
    RexCallConverter build();
  }
}
