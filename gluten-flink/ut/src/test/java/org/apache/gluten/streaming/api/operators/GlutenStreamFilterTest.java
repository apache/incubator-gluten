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
import io.github.zhztheplayer.velox4j.plan.FilterNode;
import io.github.zhztheplayer.velox4j.plan.PlanNode;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.operators.StreamFilterTest;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

public class GlutenStreamFilterTest extends StreamFilterTest {

  private static final Logger LOG = LoggerFactory.getLogger(GlutenStreamFilterTest.class);
  private static FlinkTypeFactory typeFactory;
  private static RexBuilder rexBuilder;

  @BeforeAll
  public static void setupAll() {
    LOG.info("GlutenStreamFilterTest setup");
    Velox4jEnvironment.initializeOnce();
    typeFactory =
        new FlinkTypeFactory(
            Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);
    rexBuilder = new FlinkRexBuilder(typeFactory);
  }

  @Override
  @Test
  public void testFilter() throws Exception {
    RowType flinkRowType =
        RowType.of(
            new LogicalType[] {
              new IntType(), new VarCharType(VarCharType.MAX_LENGTH), new IntType()
            },
            new String[] {"id", "name", "age"});
    List<String> inNames = Utils.getNamesFromRowType(flinkRowType);

    RexNode ageFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);
    RexNode literal18 =
        rexBuilder.makeLiteral(
            new BigDecimal(18), typeFactory.createSqlType(SqlTypeName.INTEGER), false);
    RexNode filterConditionFlink =
        rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ageFieldRef, literal18);

    RexConversionContext conversionContext = new RexConversionContext(inNames);
    TypedExpr veloxFilterCondition =
        RexNodeConverter.toTypedExpr(filterConditionFlink, conversionContext);

    io.github.zhztheplayer.velox4j.type.RowType veloxType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            org.apache.gluten.util.LogicalTypeConverter.toVLType(flinkRowType);

    PlanNode veloxPlan =
        new FilterNode(PlanNodeIdGenerator.newId(), List.of(), veloxFilterCondition);
    TypeInformation<RowData> typeInfo = InternalTypeInfo.of(flinkRowType);

    GlutenSingleInputOperator operator =
        new GlutenSingleInputOperator(veloxPlan, PlanNodeIdGenerator.newId(), veloxType, veloxType);

    TypeSerializer<RowData> serializer = typeInfo.createSerializer(new SerializerConfigImpl());
    OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(operator, serializer);

    harness.setup(typeInfo.createSerializer(new SerializerConfigImpl()));
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
            GenericRowData.of(1, StringData.fromString("Alice"), 20),
            GenericRowData.of(3, StringData.fromString("Charlie"), 25));

    assertThat(actualOutput).hasSize(expectedOutput.size());
    for (int i = 0; i < expectedOutput.size(); i++) {
      RowData expectedRow = expectedOutput.get(i);
      RowData actualRow = actualOutput.get(i);
      assertThat(actualRow.getInt(0)).isEqualTo(expectedRow.getInt(0));
      assertThat(actualRow.getString(1).toString()).isEqualTo(expectedRow.getString(1).toString());
      assertThat(actualRow.getInt(2)).isEqualTo(expectedRow.getInt(2));
    }

    harness.close();
  }

  @Override
  @Test
  public void testOpenClose() throws Exception {
    RowType flinkRowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});
    List<String> inNames = Utils.getNamesFromRowType(flinkRowType);

    RexNode idFieldRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
    RexNode literal0 =
        rexBuilder.makeLiteral(
            new BigDecimal(0), typeFactory.createSqlType(SqlTypeName.INTEGER), false);
    RexNode filterConditionFlink =
        rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, idFieldRef, literal0);

    RexConversionContext conversionContext = new RexConversionContext(inNames);
    TypedExpr veloxFilterCondition =
        RexNodeConverter.toTypedExpr(filterConditionFlink, conversionContext);

    io.github.zhztheplayer.velox4j.type.RowType veloxType =
        (io.github.zhztheplayer.velox4j.type.RowType)
            org.apache.gluten.util.LogicalTypeConverter.toVLType(flinkRowType);

    PlanNode veloxPlan =
        new FilterNode(PlanNodeIdGenerator.newId(), List.of(), veloxFilterCondition);
    TypeInformation<RowData> typeInfo = InternalTypeInfo.of(flinkRowType);

    TestableGlutenSingleInputOperator operator =
        new TestableGlutenSingleInputOperator(
            veloxPlan, PlanNodeIdGenerator.newId(), veloxType, veloxType);

    TypeSerializer<RowData> serializer = typeInfo.createSerializer(new SerializerConfigImpl());
    OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        new OneInputStreamOperatorTestHarness<>(operator, serializer);

    assertThat(operator.isOpened()).isFalse();
    assertThat(operator.isClosed()).isFalse();

    harness.setup(typeInfo.createSerializer(new SerializerConfigImpl()));
    harness.open();

    assertThat(operator.isOpened()).isTrue();
    assertThat(operator.isClosed()).isFalse();

    harness.processElement(new StreamRecord<>(GenericRowData.of(1), 0L));

    assertThat(harness.getOutput()).isNotEmpty();

    harness.close();

    assertThat(operator.isOpened()).isTrue();
    assertThat(operator.isClosed()).isTrue();
  }

  private static class TestableGlutenSingleInputOperator extends GlutenSingleInputOperator {
    private boolean opened = false;
    private boolean closed = false;

    public TestableGlutenSingleInputOperator(
        PlanNode veloxPlan,
        String planNodeId,
        io.github.zhztheplayer.velox4j.type.RowType inputType,
        io.github.zhztheplayer.velox4j.type.RowType outputType) {
      super(veloxPlan, planNodeId, inputType, outputType);
    }

    @Override
    public void open() throws Exception {
      if (closed) {
        throw new IllegalStateException("Close called before open.");
      }
      super.open();
      opened = true;
      LOG.debug("GlutenSingleInputOperator opened successfully");
    }

    @Override
    public void close() throws Exception {
      if (!opened) {
        throw new IllegalStateException("Open was not called before close.");
      }
      super.close();
      closed = true;
      LOG.debug("GlutenSingleInputOperator closed successfully");
    }

    public boolean isOpened() {
      return opened;
    }

    public boolean isClosed() {
      return closed;
    }
  }
}
