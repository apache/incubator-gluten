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
import org.apache.gluten.table.runtime.operators.GlutenVectorTwoInputOperator;
import org.apache.gluten.table.runtime.stream.common.Velox4jEnvironment;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;
import org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.connector.ExternalStreamTableHandle;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.join.JoinType;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.NestedLoopJoinNode;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.StreamJoinNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;
import io.github.zhztheplayer.velox4j.type.BooleanType;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class GlutenStreamJoinOperatorTestBase {

  protected static FlinkTypeFactory typeFactory;
  protected static RexBuilder rexBuilder;

  protected static org.apache.flink.table.types.logical.RowType leftRowType;
  protected static InternalTypeInfo<RowData> leftTypeInfo;
  protected static RowType leftVeloxType;

  protected static org.apache.flink.table.types.logical.RowType rightRowType;
  protected static InternalTypeInfo<RowData> rightTypeInfo;
  protected static RowType rightVeloxType;

  protected static org.apache.flink.table.types.logical.RowType outputRowType;
  protected static InternalTypeInfo<RowData> outputTypeInfo;
  protected static RowType outputVeloxType;

  protected static final int[] leftJoinKeys = {1};
  protected static final int[] rightJoinKeys = {0};

  protected static List<RowData> leftTestData;
  protected static List<RowData> rightTestData;

  protected MemoryManager sharedMemoryManager;
  protected Session sharedSession;
  protected BufferAllocator sharedAllocator;

  @BeforeAll
  public static void setupJoinTestEnvironment() {
    Velox4jEnvironment.initializeOnce();

    typeFactory =
        new FlinkTypeFactory(
            Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);
    rexBuilder = new FlinkRexBuilder(typeFactory);

    leftRowType =
        org.apache.flink.table.types.logical.RowType.of(
            new LogicalType[] {
              new CharType(false, 20), new CharType(false, 20), VarCharType.STRING_TYPE
            },
            new String[] {"order_id", "line_order_id", "shipping_address"});
    leftTypeInfo = InternalTypeInfo.of(leftRowType);
    leftVeloxType = (RowType) LogicalTypeConverter.toVLType(leftRowType);

    rightRowType =
        org.apache.flink.table.types.logical.RowType.of(
            new LogicalType[] {new CharType(false, 20), new CharType(true, 10)},
            new String[] {"line_order_id0", "line_order_ship_mode"});
    rightTypeInfo = InternalTypeInfo.of(rightRowType);
    rightVeloxType = (RowType) LogicalTypeConverter.toVLType(rightRowType);

    outputRowType =
        org.apache.flink.table.types.logical.RowType.of(
            Stream.concat(leftRowType.getChildren().stream(), rightRowType.getChildren().stream())
                .toArray(LogicalType[]::new),
            Stream.concat(
                    leftRowType.getFieldNames().stream(), rightRowType.getFieldNames().stream())
                .toArray(String[]::new));
    outputTypeInfo = InternalTypeInfo.of(outputRowType);
    outputVeloxType = (RowType) LogicalTypeConverter.toVLType(outputRowType);

    setupTestData();
  }

  @BeforeEach
  public void setupResources() {
    try {
      sharedMemoryManager = MemoryManager.create(AllocationListener.NOOP);
      sharedSession = Velox4j.newSession(sharedMemoryManager);
      sharedAllocator = new RootAllocator(Long.MAX_VALUE);
    } catch (Exception e) {
      throw new RuntimeException("Failed to setup test resources", e);
    }
  }

  @AfterEach
  public void cleanupResources() {
    try {
      if (sharedAllocator != null) {
        sharedAllocator.close();
        sharedAllocator = null;
      }
      if (sharedSession != null) {
        sharedSession.close();
        sharedSession = null;
      }
      if (sharedMemoryManager != null) {
        sharedMemoryManager.close();
        sharedMemoryManager = null;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void setupTestData() {
    leftTestData =
        Arrays.asList(
            GenericRowData.of(
                StringData.fromString("Ord#1"),
                StringData.fromString("LineOrd#1"),
                StringData.fromString("3 Bellevue Drive, Pottstown, PA 19464")),
            GenericRowData.of(
                StringData.fromString("Ord#1"),
                StringData.fromString("LineOrd#2"),
                StringData.fromString("3 Bellevue Drive, Pottstown, PA 19464")),
            GenericRowData.of(
                StringData.fromString("Ord#2"),
                StringData.fromString("LineOrd#3"),
                StringData.fromString("68 Manor Station Street, Honolulu, HI 96815")));

    rightTestData =
        Arrays.asList(
            GenericRowData.of(StringData.fromString("LineOrd#2"), StringData.fromString("AIR")),
            GenericRowData.of(StringData.fromString("LineOrd#3"), StringData.fromString("TRUCK")),
            GenericRowData.of(StringData.fromString("LineOrd#4"), StringData.fromString("SHIP")));
  }

  protected GlutenVectorTwoInputOperator createJoinOperator(FlinkJoinType joinType) {
    return createJoinOperator(joinType, null);
  }

  protected GlutenVectorTwoInputOperator createJoinOperator(
      FlinkJoinType joinType, RexNode nonEquiCondition) {
    JoinType veloxJoinType = Utils.toVLJoinType(joinType);

    List<FieldAccessTypedExpr> leftKeys =
        Utils.analyzeJoinKeys(leftVeloxType, leftJoinKeys, List.of());
    List<FieldAccessTypedExpr> rightKeys =
        Utils.analyzeJoinKeys(rightVeloxType, rightJoinKeys, List.of());

    TypedExpr joinCondition = Utils.generateJoinEqualCondition(leftKeys, rightKeys);

    if (nonEquiCondition != null) {
      RexConversionContext conversionContext = new RexConversionContext(outputVeloxType.getNames());
      TypedExpr nonEqual = RexNodeConverter.toTypedExpr(nonEquiCondition, conversionContext);
      joinCondition = new CallTypedExpr(new BooleanType(), List.of(joinCondition, nonEqual), "and");
    }

    PlanNode leftInput =
        new TableScanNode(
            PlanNodeIdGenerator.newId(),
            leftVeloxType,
            new ExternalStreamTableHandle("connector-external-stream"),
            List.of());

    PlanNode rightInput =
        new TableScanNode(
            PlanNodeIdGenerator.newId(),
            rightVeloxType,
            new ExternalStreamTableHandle("connector-external-stream"),
            List.of());

    NestedLoopJoinNode leftNode =
        new NestedLoopJoinNode(
            PlanNodeIdGenerator.newId(),
            veloxJoinType,
            joinCondition,
            new EmptyNode(leftVeloxType),
            new EmptyNode(rightVeloxType),
            outputVeloxType);

    NestedLoopJoinNode rightNode =
        new NestedLoopJoinNode(
            PlanNodeIdGenerator.newId(),
            veloxJoinType,
            joinCondition,
            new EmptyNode(rightVeloxType),
            new EmptyNode(leftVeloxType),
            outputVeloxType);

    PlanNode join =
        new StreamJoinNode(
            PlanNodeIdGenerator.newId(),
            leftInput,
            rightInput,
            leftNode,
            rightNode,
            outputVeloxType);

    return new GlutenVectorTwoInputOperator(
        new StatefulPlanNode(join.getId(), join),
        leftInput.getId(),
        rightInput.getId(),
        leftVeloxType,
        rightVeloxType,
        Map.of(join.getId(), outputVeloxType));
  }

  protected TwoInputStreamOperatorTestHarness<StatefulRecord, StatefulRecord, RowData>
      createTestHarness(GlutenVectorTwoInputOperator operator) throws Exception {

    TwoInputStreamOperatorTestHarness<StatefulRecord, StatefulRecord, RowData> harness =
        new TwoInputStreamOperatorTestHarness<>(operator);

    harness.setup();
    harness.open();

    return harness;
  }

  protected StatefulRecord convertToStatefulRecord(RowData rowData, RowType rowType) {
    try {
      RowVector rowVector =
          FlinkRowToVLVectorConvertor.fromRowData(rowData, sharedAllocator, sharedSession, rowType);

      StatefulRecord record = new StatefulRecord(null, 0, 0, false, -1);
      record.setRowVector(rowVector);

      return record;
    } catch (Exception e) {
      throw new RuntimeException("Failed to convert RowData to StatefulRecord", e);
    }
  }

  protected void processLeftTestData(
      TwoInputStreamOperatorTestHarness<StatefulRecord, StatefulRecord, RowData> harness,
      List<RowData> leftData)
      throws Exception {
    long timestamp = 0L;
    for (RowData row : leftData) {
      StatefulRecord record = convertToStatefulRecord(row, leftVeloxType);
      harness.processElement1(new StreamRecord<>(record, timestamp++));
    }
  }

  protected void processRightTestData(
      TwoInputStreamOperatorTestHarness<StatefulRecord, StatefulRecord, RowData> harness,
      List<RowData> rightData)
      throws Exception {
    long timestamp = 0L;
    for (RowData row : rightData) {
      StatefulRecord record = convertToStatefulRecord(row, rightVeloxType);
      harness.processElement2(new StreamRecord<>(record, timestamp++));
    }
  }

  protected List<RowData> extractOutputFromHarness(
      TwoInputStreamOperatorTestHarness<StatefulRecord, StatefulRecord, RowData> harness) {
    Queue<Object> outputQueue = harness.getOutput();
    return outputQueue.stream()
        .filter(record -> record instanceof StreamRecord)
        .map(record -> ((StreamRecord<RowData>) record).getValue())
        .collect(Collectors.toList());
  }

  protected RexNode createNonEquiCondition() {
    RexNode leftField = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.CHAR, 20), 0);
    RexNode rightField =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.CHAR, 10), 4);
    return rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, leftField, rightField);
  }
}
