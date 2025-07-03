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
package org.apache.gluten.streaming.api.operators;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.configuration.Configuration;
import org.apache.calcite.rex.RexNode;
import org.apache.gluten.streaming.api.operators.utils.RowDataTestUtils;

import java.util.List;
import java.util.ArrayList;

public abstract class GlutenStreamOperatorTestBase {

  protected OneInputStreamOperatorTestHarness<RowData, RowData> harness;
  protected RowType inputRowType;
  protected RowType outputRowType;
  protected TypeInformation<RowData> inputTypeInfo;
  protected TypeInformation<RowData> outputTypeInfo;

  protected void setupTestHarness(StreamOperator<RowData> operator) throws Exception {
    TypeSerializer<RowData> serializer =
            inputTypeInfo.createSerializer(new SerializerConfigImpl());
    harness = new OneInputStreamOperatorTestHarness<>((OneInputStreamOperator) operator, serializer);
    harness.setup();
    harness.open();
  }

  protected void processElement(RowData input, long timestamp) throws Exception {
    harness.processElement(new StreamRecord<>(input, timestamp));
  }

  protected List<RowData> getOutputValues() {
    List<RowData> result = new ArrayList<>();
    for (StreamRecord<? extends RowData> record : harness.extractOutputStreamRecords()) {
      result.add(record.getValue());
    }
    return result;
  }

  protected void tearDown() throws Exception {
    if (harness != null) {
      harness.close();
    }
  }

  protected TableStreamOperator<RowData> createProjectOperator(
          RowType inputType, RowType outputType, int[] projectionFields) {

    CodeGeneratorContext ctx = new CodeGeneratorContext(new Configuration());
    GeneratedProjection generatedProjection = ProjectionCodeGenerator.generateProjection(
            ctx, "TestProjection", inputType, outputType, projectionFields);

    Projection<RowData, RowData> projection = generatedProjection.newInstance(
            Thread.currentThread().getContextClassLoader());

    return new TableStreamOperator<>(projection);
  }

  protected void testFilter(RexNode filterCondition, List<RowData> inputData,
                            List<RowData> expectedOutput) throws Exception {

    TableStreamOperator<RowData> operator = createFilterOperator(filterCondition);
    setupTestHarness(operator);

    for (RowData input : inputData) {
      processElement(input, System.currentTimeMillis());
    }

    List<RowData> actualOutput = getOutputValues();
    RowDataTestUtils.assertRowDataListEquals(expectedOutput, actualOutput, outputRowType);

    tearDown();
  }

  protected abstract TableStreamOperator<RowData> createFilterOperator(RexNode filterCondition);
}

