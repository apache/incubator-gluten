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

import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.FilterNode;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;

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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

public class GlutenStreamFilterTest {

  private static FlinkTypeFactory typeFactory;
  private static RexBuilder rexBuilder;
  private static RowType flinkRowType;
  private static TypeInformation<RowData> typeInfo;
  private static List<RowData> testInputData;

  @BeforeAll
  public static void setupAll() {
    Velox4jEnvironment.initializeOnce();
    typeFactory =
        new FlinkTypeFactory(
            Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);
    rexBuilder = new FlinkRexBuilder(typeFactory);

    flinkRowType =
        RowType.of(
            new LogicalType[] {
              new IntType(), new VarCharType(VarCharType.MAX_LENGTH), new IntType()
            },
            new String[] {"id", "name", "age"});

    typeInfo = InternalTypeInfo.of(flinkRowType);

    testInputData =
        Arrays.asList(
            GenericRowData.of(1, StringData.fromString("Alice"), 20),
            GenericRowData.of(2, StringData.fromString("Bob"), 17),
            GenericRowData.of(3, StringData.fromString("Charlie"), 25),
            GenericRowData.of(4, StringData.fromString("David"), 15));
  }

  @Test
  public void testGreaterThanFilter() throws Exception {
    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(1, StringData.fromString("Alice"), 20),
            GenericRowData.of(3, StringData.fromString("Charlie"), 25));

    testComparisonFilter(SqlStdOperatorTable.GREATER_THAN, 18, expectedOutput);
  }

  @Test
  public void testLessThanFilter() throws Exception {
    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(2, StringData.fromString("Bob"), 17),
            GenericRowData.of(4, StringData.fromString("David"), 15));

    testComparisonFilter(SqlStdOperatorTable.LESS_THAN, 20, expectedOutput);
  }

  @Test
  public void testEqualToFilter() throws Exception {
    List<RowData> expectedOutput =
        Arrays.asList(GenericRowData.of(1, StringData.fromString("Alice"), 20));

    testComparisonFilter(SqlStdOperatorTable.EQUALS, 20, expectedOutput);
  }

  @Test
  @Disabled
  public void testGreaterThanOrEqualFilter() throws Exception {
    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(1, StringData.fromString("Alice"), 20),
            GenericRowData.of(3, StringData.fromString("Charlie"), 25));

    testComparisonFilter(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, 20, expectedOutput);
  }

  @Test
  @Disabled
  public void testLessThanOrEqualFilter() throws Exception {
    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(1, StringData.fromString("Alice"), 20),
            GenericRowData.of(2, StringData.fromString("Bob"), 17),
            GenericRowData.of(4, StringData.fromString("David"), 15));

    testComparisonFilter(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, 20, expectedOutput);
  }

  @Test
  @Disabled
  public void testNotEqualFilter() throws Exception {
    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(2, StringData.fromString("Bob"), 17),
            GenericRowData.of(3, StringData.fromString("Charlie"), 25),
            GenericRowData.of(4, StringData.fromString("David"), 15));

    testComparisonFilter(SqlStdOperatorTable.NOT_EQUALS, 20, expectedOutput);
  }

  @Test
  public void testStringEqualFilter() throws Exception {

    RexNode nameFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
    RexNode name =
        rexBuilder.makeLiteral("Alice", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
    RexNode flinkFilterCondition =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, nameFieldRef, name);

    List<RowData> expectedOutput =
        Arrays.asList(GenericRowData.of(1, StringData.fromString("Alice"), 20));

    testFilterWithCondition(flinkFilterCondition, expectedOutput);
  }

  @Test
  public void testOpenClose() throws Exception {
    RowType simpleRowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});
    List<String> inNames = Utils.getNamesFromRowType(simpleRowType);

    RexNode idFieldRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
    RexNode id = rexBuilder.makeLiteral(0, typeFactory.createSqlType(SqlTypeName.INTEGER), false);
    RexNode flinkFilterCondition =
        rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, idFieldRef, id);

    RexConversionContext conversionContext = new RexConversionContext(inNames);
    TypedExpr veloxFilterCondition =
        RexNodeConverter.toTypedExpr(flinkFilterCondition, conversionContext);

    io.github.zhztheplayer.velox4j.type.RowType veloxType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            org.apache.gluten.util.LogicalTypeConverter.toVLType(simpleRowType);

    PlanNode veloxPlan =
        new FilterNode(
            PlanNodeIdGenerator.newId(), List.of(new EmptyNode(veloxType)), veloxFilterCondition);

    TestableGlutenSingleInputOperator operator =
        new TestableGlutenSingleInputOperator(
            veloxPlan, PlanNodeIdGenerator.newId(), veloxType, veloxType);

    TypeInformation<RowData> simpleTypeInfo = InternalTypeInfo.of(simpleRowType);
    TypeSerializer<RowData> serializer =
        simpleTypeInfo.createSerializer(new SerializerConfigImpl());
    OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(operator, serializer);

    assertThat(operator.isOpened()).isFalse();
    assertThat(operator.isClosed()).isFalse();

    harness.setup(simpleTypeInfo.createSerializer(new SerializerConfigImpl()));
    harness.open();

    assertThat(operator.isOpened()).isTrue();
    assertThat(operator.isClosed()).isFalse();

    harness.processElement(new StreamRecord<>(GenericRowData.of(1), 0L));

    assertThat(harness.getOutput()).isNotEmpty();

    harness.close();

    assertThat(operator.isOpened()).isTrue();
    assertThat(operator.isClosed()).isTrue();
  }

  private void testComparisonFilter(
      SqlOperator operator, int compareValue, List<RowData> expectedOutput) throws Exception {

    RexNode ageFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
    RexNode literal =
        rexBuilder.makeLiteral(compareValue, typeFactory.createSqlType(SqlTypeName.INTEGER), false);
    RexNode flinkFilterCondition = rexBuilder.makeCall(operator, ageFieldRef, literal);

    testFilterWithCondition(flinkFilterCondition, expectedOutput);
  }

  private void testFilterWithCondition(RexNode filterCondition, List<RowData> expectedOutput)
      throws Exception {
    List<String> inNames = Utils.getNamesFromRowType(flinkRowType);

    RexConversionContext conversionContext = new RexConversionContext(inNames);
    TypedExpr veloxFilterCondition =
        RexNodeConverter.toTypedExpr(filterCondition, conversionContext);

    io.github.zhztheplayer.velox4j.type.RowType veloxType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            org.apache.gluten.util.LogicalTypeConverter.toVLType(flinkRowType);

    PlanNode veloxPlan =
        new FilterNode(
            PlanNodeIdGenerator.newId(), List.of(new EmptyNode(veloxType)), veloxFilterCondition);

    GlutenSingleInputOperator operator =
        new TestGlutenSingleInputOperator(
            new StatefulPlanNode(veloxPlan.getId(), veloxPlan),
            PlanNodeIdGenerator.newId(),
            veloxType,
            Map.of(veloxPlan.getId(), veloxType));

    TypeSerializer<RowData> serializer = typeInfo.createSerializer(new SerializerConfigImpl());
    OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(operator, serializer);

    harness.setup(typeInfo.createSerializer(new SerializerConfigImpl()));
    harness.open();

    long timestamp = 0L;
    for (RowData row : testInputData) {
      harness.processElement(new StreamRecord<>(row, timestamp++));
    }

    List<RowData> actualOutput = extractOutputFromHarness(harness);
    assertRowDataListEquals(actualOutput, expectedOutput);

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

  private List<RowData> extractOutputFromHarness(
      OneInputStreamOperatorTestHarness<RowData, RowData> harness) {
    Queue<Object> outputQueue = harness.getOutput();
    List<RowData> actualOutput = new ArrayList<>();
    while (!outputQueue.isEmpty()) {
      Object record = outputQueue.poll();
      if (record instanceof StreamRecord) {
        actualOutput.add(((StreamRecord<RowData>) record).getValue());
      }
    }
    return actualOutput;
  }

  private void assertRowDataListEquals(List<RowData> actual, List<RowData> expected) {
    assertThat(actual).hasSize(expected.size());
    for (int i = 0; i < expected.size(); i++) {
      assertRowDataEquals(actual.get(i), expected.get(i));
    }
  }

  private void assertRowDataEquals(RowData actual, RowData expected) {
    assertThat(actual.getInt(0)).isEqualTo(expected.getInt(0));
    assertThat(actual.getString(1).toString()).isEqualTo(expected.getString(1).toString());
    assertThat(actual.getInt(2)).isEqualTo(expected.getInt(2));
  }

  private static class TestableGlutenSingleInputOperator extends TestGlutenSingleInputOperator {
    private boolean opened = false;
    private boolean closed = false;

    public TestableGlutenSingleInputOperator(
        PlanNode veloxPlan,
        String planNodeId,
        io.github.zhztheplayer.velox4j.type.RowType inputType,
        io.github.zhztheplayer.velox4j.type.RowType outputType) {
      super(
          new StatefulPlanNode(veloxPlan.getId(), veloxPlan),
          planNodeId,
          inputType,
          Map.of(veloxPlan.getId(), outputType));
    }

    @Override
    public void open() throws Exception {
      if (closed) {
        throw new IllegalStateException("Close called before open.");
      }
      super.open();
      opened = true;
    }

    @Override
    public void close() throws Exception {
      if (!opened) {
        throw new IllegalStateException("Open was not called before close.");
      }
      super.close();
      closed = true;
    }

    public boolean isOpened() {
      return opened;
    }

    public boolean isClosed() {
      return closed;
    }
  }
}
