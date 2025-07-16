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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.AbstractStreamingJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperatorTestBase;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.calcite.rex.RexNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.gluten.streaming.api.operators.utils.RowDataTestUtils.checkEquals;

public abstract class GlutenStreamJoinOperatorTestBase extends StreamingJoinOperatorTestBase {

  protected static RowType leftVeloxType;
  protected static RowType rightVeloxType;
  protected static RowType outputVeloxType;
  protected static org.apache.flink.table.types.logical.RowType outputRowType;

  protected MemoryManager sharedMemoryManager;
  protected Session sharedSession;
  protected BufferAllocator sharedAllocator;

  @BeforeAll
  public static void setupGlutenEnvironment() {
    Velox4jEnvironment.initializeOnce();
  }

  @Override
  @BeforeEach
  public void beforeEach(TestInfo testInfo) throws Exception {
    setupGlutenResources();
    setupGlutenTypes();
  }

  @Override
  @AfterEach
  public void afterEach() throws Exception {
    cleanupGlutenResources();
  }

  private void setupGlutenTypes() {
    leftVeloxType = (RowType) LogicalTypeConverter.toVLType(leftTypeInfo.toRowType());
    rightVeloxType = (RowType) LogicalTypeConverter.toVLType(rightTypeInfo.toRowType());

    outputRowType =
        org.apache.flink.table.types.logical.RowType.of(
            Stream.concat(
                    leftTypeInfo.toRowType().getChildren().stream(),
                    rightTypeInfo.toRowType().getChildren().stream())
                .toArray(LogicalType[]::new),
            Stream.concat(
                    leftTypeInfo.toRowType().getFieldNames().stream(),
                    rightTypeInfo.toRowType().getFieldNames().stream())
                .toArray(String[]::new));

    outputVeloxType = (RowType) LogicalTypeConverter.toVLType(outputRowType);
  }

  @Override
  protected final AbstractStreamingJoinOperator createJoinOperator(TestInfo testInfo) {
    throw new UnsupportedOperationException("Use createGlutenJoinOperator instead");
  }

  @Override
  protected org.apache.flink.table.types.logical.RowType getOutputType() {
    return outputRowType;
  }

  protected GlutenVectorTwoInputOperator createGlutenJoinOperator(FlinkJoinType joinType) {
    return createGlutenJoinOperator(joinType, null);
  }

  protected GlutenVectorTwoInputOperator createGlutenJoinOperator(
      FlinkJoinType joinType, RexNode nonEquiCondition) {
    JoinType veloxJoinType = Utils.toVLJoinType(joinType);

    int[] leftJoinKeys = {1};
    int[] rightJoinKeys = {0};

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

  protected void processTestData(
      TwoInputStreamOperatorTestHarness<StatefulRecord, StatefulRecord, RowData> harness,
      List<RowData> leftData,
      List<RowData> rightData)
      throws Exception {
    long timestamp = 0L;
    for (RowData row : leftData) {
      StatefulRecord record = convertToStatefulRecord(row, leftVeloxType);
      harness.processElement1(new StreamRecord<>(record, timestamp++));
    }

    timestamp = 0L;
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

  private void setupGlutenResources() {
    sharedMemoryManager = MemoryManager.create(AllocationListener.NOOP);
    sharedSession = Velox4j.newSession(sharedMemoryManager);
    sharedAllocator = new RootAllocator(Long.MAX_VALUE);
  }

  private void cleanupGlutenResources() {
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

  private StatefulRecord convertToStatefulRecord(RowData rowData, RowType rowType) {
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

  protected void executeJoinTest(
      GlutenVectorTwoInputOperator operator,
      List<RowData> leftData,
      List<RowData> rightData,
      List<RowData> expectedOutput)
      throws Exception {
    TwoInputStreamOperatorTestHarness<StatefulRecord, StatefulRecord, RowData> harness =
        new TwoInputStreamOperatorTestHarness<>(operator);
    try {
      harness.setup();
      harness.open();

      processTestData(harness, leftData, rightData);
      List<RowData> actualOutput = extractOutputFromHarness(harness);
      checkEquals(actualOutput, expectedOutput, outputRowType.getChildren());
    } finally {
      harness.close();
    }
  }
}
