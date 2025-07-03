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

import org.apache.gluten.rexnode.RexConversionContext;
import org.apache.gluten.rexnode.RexNodeConverter;
import org.apache.gluten.rexnode.Utils;
import org.apache.gluten.table.runtime.operators.GlutenSingleInputOperator;
import org.apache.gluten.table.runtime.stream.common.Velox4jEnvironment;
import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.connector.ExternalStreamTableHandle;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.plan.*;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

public class GlutenStreamProjectTest {
  private static FlinkTypeFactory typeFactory;
  private static RexBuilder rexBuilder;

  @BeforeAll
  public static void setupAll() {
    Velox4jEnvironment.initializeOnce();
    typeFactory =
        new FlinkTypeFactory(
            Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);
    rexBuilder = new FlinkRexBuilder(typeFactory);
  }

  @Test
  public void testProject() throws Exception {
    RowType flinkInputRowType =
        RowType.of(
            new LogicalType[] {
              new IntType(), new VarCharType(VarCharType.MAX_LENGTH), new IntType()
            },
            new String[] {"id", "name", "age"});

    RowType flinkOutputRowType =
        RowType.of(
            new LogicalType[] {new VarCharType(VarCharType.MAX_LENGTH), new IntType()},
            new String[] {"projected_name", "age_multiplied"});

    List<String> inNames = Utils.getNamesFromRowType(flinkInputRowType);

    RexNode nameFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
    RexNode ageFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
    RexNode literal2 =
        rexBuilder.makeLiteral(2, typeFactory.createSqlType(SqlTypeName.INTEGER), false);
    RexNode ageMultipliedExpr =
        rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, ageFieldRef, literal2);

    List<RexNode> projectionsFlink = Arrays.asList(nameFieldRef, ageMultipliedExpr);
    List<String> projectedFieldNames = Arrays.asList("projected_name", "age_multiplied");

    RexConversionContext conversionContext = new RexConversionContext(inNames);
    List<TypedExpr> veloxProjections =
        RexNodeConverter.toTypedExpr(projectionsFlink, conversionContext);

    io.github.zhztheplayer.velox4j.type.RowType veloxInputType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            org.apache.gluten.util.LogicalTypeConverter.toVLType(flinkInputRowType);

    io.github.zhztheplayer.velox4j.type.RowType veloxOutputType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            org.apache.gluten.util.LogicalTypeConverter.toVLType(flinkOutputRowType);

    PlanNode mockInput =
        new TableScanNode(
            PlanNodeIdGenerator.newId(),
            veloxInputType,
            new ExternalStreamTableHandle("connector-external-stream"),
            List.of());
    PlanNode veloxPlan =
        new ProjectNode(
            PlanNodeIdGenerator.newId(), List.of(mockInput), projectedFieldNames, veloxProjections);

    TypeInformation<RowData> inputTypeInfo = InternalTypeInfo.of(flinkInputRowType);
    TypeInformation<RowData> outputTypeInfo = InternalTypeInfo.of(flinkOutputRowType);

    GlutenSingleInputOperator operator =
        new TestGlutenSingleInputOperator(
            new StatefulPlanNode(veloxPlan.getId(), veloxPlan),
            PlanNodeIdGenerator.newId(),
            veloxInputType,
            Map.of(veloxPlan.getId(), veloxOutputType));

    TypeSerializer<RowData> inputSerializer =
        inputTypeInfo.createSerializer(new SerializerConfigImpl());
    OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(operator, inputSerializer);

    harness.setup(outputTypeInfo.createSerializer(new SerializerConfigImpl()));
    harness.open();

    List<RowData> inputData =
        Arrays.asList(
            GenericRowData.of(1, StringData.fromString("Alice"), 20),
            GenericRowData.of(2, StringData.fromString("Bob"), 17),
            GenericRowData.of(3, StringData.fromString("Charlie"), 25),
            GenericRowData.of(4, StringData.fromString("David"), 15));

    long timestamp = 0L;
    for (RowData row : inputData) {
      harness.processElement(new StreamRecord<>(row, timestamp++));
    }

    Queue<Object> outputQueue = harness.getOutput();
    List<RowData> actualOutput = new ArrayList<>();
    while (!outputQueue.isEmpty()) {
      Object record = outputQueue.poll();
      if (record instanceof StreamRecord) {
        actualOutput.add(((StreamRecord<RowData>) record).getValue());
      }
    }

    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(StringData.fromString("Alice"), 40),
            GenericRowData.of(StringData.fromString("Bob"), 34),
            GenericRowData.of(StringData.fromString("Charlie"), 50),
            GenericRowData.of(StringData.fromString("David"), 30));

    assertThat(actualOutput).hasSize(expectedOutput.size());
    for (int i = 0; i < expectedOutput.size(); i++) {
      RowData expectedRow = expectedOutput.get(i);
      RowData actualRow = actualOutput.get(i);
      assertThat(actualRow.getString(0).toString()).isEqualTo(expectedRow.getString(0).toString());
      assertThat(actualRow.getInt(1)).isEqualTo(expectedRow.getInt(1));
    }
    harness.close();
  }

  private static class TestGlutenSingleInputOperator extends GlutenSingleInputOperator {
    public TestGlutenSingleInputOperator(
        StatefulPlanNode plan,
        String id,
        io.github.zhztheplayer.velox4j.type.RowType inputType,
        Map<String, io.github.zhztheplayer.velox4j.type.RowType> outTypes) {
      super(plan, id, inputType, outTypes);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) {
      try {
        super.processElement(element);
      } catch (Exception e) {
        if (e.getMessage() != null
            && (e.getMessage().contains("ResourceHandle not found")
                || e.getMessage().contains("Failed to reattach current thread to JVM"))) {
        } else {
          throw e;
        }
      }
    }
  }
}
