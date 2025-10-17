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

import org.apache.gluten.rexnode.Utils;
import org.apache.gluten.table.runtime.operators.GlutenOneInputOperator;
import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.ProjectNode;

import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.gluten.streaming.api.operators.utils.RowDataTestUtils.checkEquals;

public class GlutenStreamProjectTest extends GlutenStreamOperatorTestBase {

  @Test
  public void testProject() throws Exception {
    RowType outputRowType =
        RowType.of(
            new LogicalType[] {new VarCharType(VarCharType.MAX_LENGTH), new IntType()},
            new String[] {"projected_name", "age_multiplied"});

    List<RexNode> expressions = createProjectionExpressions();
    List<String> fieldNames = Arrays.asList("projected_name", "age_multiplied");

    List<String> inputFieldNames = Utils.getNamesFromRowType(rowType);
    List<TypedExpr> veloxProjections = convertRexListToVelox(expressions, inputFieldNames);

    PlanNode veloxPlan =
        new ProjectNode(
            PlanNodeIdGenerator.newId(),
            List.of(new EmptyNode(veloxType)),
            fieldNames,
            veloxProjections);

    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(StringData.fromString("Alice"), 40),
            GenericRowData.of(StringData.fromString("Bob"), 34),
            GenericRowData.of(StringData.fromString("Charlie"), 50),
            GenericRowData.of(StringData.fromString("David"), 30),
            GenericRowData.of(null, 60),
            GenericRowData.of(StringData.fromString("Eve"), null),
            GenericRowData.of(null, null),
            GenericRowData.of(StringData.fromString("Frank"), 44),
            GenericRowData.of(null, null));

    GlutenOneInputOperator operator =
        createTestOperator(veloxPlan, typeInfo, InternalTypeInfo.of(outputRowType));

    OneInputStreamOperatorTestHarness<RowData, RowData> harness =
        createTestHarness(operator, typeInfo, InternalTypeInfo.of(outputRowType));

    processTestData(harness, testData);

    List<RowData> actualOutput = extractOutputFromHarness(harness);

    checkEquals(actualOutput, expectedOutput, outputRowType.getChildren());

    harness.close();
  }

  private List<RexNode> createProjectionExpressions() {
    RexNode nameFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);

    RexNode ageFieldRef =
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2);

    RexNode literal2 =
        rexBuilder.makeLiteral(2, typeFactory.createSqlType(SqlTypeName.INTEGER), false);

    RexNode ageMultipliedExpr =
        rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, ageFieldRef, literal2);

    return Arrays.asList(nameFieldRef, ageMultipliedExpr);
  }
}
