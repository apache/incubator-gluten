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

package com.intel.oap.expression;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.ArrowBuf;
import com.google.common.collect.Lists;

import org.apache.arrow.gandiva.evaluator.*;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils;

public class ColumnarArithmeticWithGandiva implements AutoCloseable {
  protected static ArrowType int32 = new ArrowType.Int(32, true);
  protected static ArrowType float32 = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
  protected static long make_time = 0;
  protected static long evaluate_time = 0;

  @Override
  public void close() throws IOException {
  }

  public static void columnarAdd(
      Schema schema,
      List<ExpressionTree> exprs,
      int numRowsInBatch,
      List<ValueVector> inputVectors,
      List<ValueVector> outputVectors)
      throws GandivaException, Exception {
    long start = System.nanoTime();
    Projector projector = Projector.make(schema, exprs);
    make_time += System.nanoTime() - start;

    start = System.nanoTime();
    List<ArrowFieldNode> fieldNodes = new ArrayList<ArrowFieldNode>();
    List<ArrowBuf> inputData = new ArrayList<ArrowBuf>();
    for (int i = 0; i < inputVectors.size(); i++) {
      fieldNodes.add(new ArrowFieldNode(numRowsInBatch, inputVectors.get(i).getNullCount()));
      inputData.add(inputVectors.get(i).getValidityBuffer());
      inputData.add(inputVectors.get(i).getDataBuffer());
    }
    ArrowRecordBatch inputRecordBatch = new ArrowRecordBatch(numRowsInBatch, fieldNodes, inputData);
    projector.evaluate(inputRecordBatch, outputVectors);
    evaluate_time += System.nanoTime() - start;

    projector.close();
  }

  /** ******* For Test ******** */
  public static void columnarBatchAdd(
      Schema schema,
      List<ExpressionTree> exprs,
      List<ArrowColumnarBatch> inputBatchs,
      List<ArrowColumnarBatch> outputBatchs)
      throws GandivaException, Exception {
    // start test
    for (int i = 0; i < inputBatchs.size(); i++) {
      columnarAdd(
          schema,
          exprs,
          inputBatchs.get(i).numRowsInBatch,
          inputBatchs.get(i).valueVectors,
          outputBatchs.get(i).valueVectors);
    }
    long make_elapsedTime = TimeUnit.NANOSECONDS.toMillis(make_time);
    long evaluate_elapsedTime = TimeUnit.NANOSECONDS.toMillis(evaluate_time);
    // long elapsedTime = finish - start;
    System.out.println(
        "ColumnarAdd process time is: "
            + evaluate_elapsedTime
            + " ms. And JIT time is "
            + make_elapsedTime
            + " ms.");
  }

  private static ExpressionTree produceAddExpressionTree(
      List<Field> dataType, List<String> ops, Field result) {
    if (dataType.size() < 2) {
      System.out.println("dataType size is less than 2");
      return null;
    }

    TreeNode last_node =
        TreeBuilder.makeFunction(
            ops.get(0),
            Lists.newArrayList(
                TreeBuilder.makeField(dataType.get(0)), TreeBuilder.makeField(dataType.get(1))),
            float32);
    TreeNode cur_node = last_node;

    for (int i = 2; i < dataType.size(); i++) {
      cur_node =
          TreeBuilder.makeFunction(
              ops.get(i - 1),
              Lists.newArrayList(last_node, TreeBuilder.makeField(dataType.get(i))),
              float32);
      last_node = cur_node;
    }
    return TreeBuilder.makeExpression(cur_node, result);
  }

  public static ArrowBuf arrowBufWithAllValid(int size) {
    int bufLen = (size + 7) / 8;
    BufferAllocator allocator = SparkMemoryUtils.contextAllocator();
    ArrowBuf buffer = allocator.buffer(bufLen);
    for (int i = 0; i < bufLen; i++) {
      buffer.writeByte(255);
    }

    return buffer;
  }

  public static void releaseRecordBatch(ArrowRecordBatch recordBatch) {
    // There are 2 references to the buffers
    // One in the recordBatch - release that by calling close()
    // One in the allocator - release that explicitly
    List<ArrowBuf> buffers = recordBatch.getBuffers();
    recordBatch.close();
    for (ArrowBuf buf : buffers) {
      buf.getReferenceManager().release();
    }
  }

  public static void main(String[] args) {
    /** ***** start preparation ****** */
    // prepare expr tree
    List<Field> dataType = new ArrayList<Field>();
    List<String> ops = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      // dataType.add(Field.nullable("n"+i, int32));
      dataType.add(Field.nullable("n" + i, float32));
      if (i > 0) {
        ops.add("add");
      }
    }
    Schema schema = new Schema(dataType);
    // Field result = Field.nullable("result", int32);
    Field result = Field.nullable("result", float32);
    ExpressionTree expr = produceAddExpressionTree(dataType, ops, result);
    List<ExpressionTree> exprs = Lists.newArrayList(expr);

    // set data scale
    int numRows = 200 * 1024 * 1024;
    int maxRowsInBatch = 16 * 1024;
    // int inputFieldSize = 4;
    int inputFieldSize = 4;

    // prepara data
    List<ArrowColumnarBatch> inputArrowColumnarBatchs = new ArrayList<ArrowColumnarBatch>();
    List<ArrowColumnarBatch> outputArrowColumnarBatchs = new ArrayList<ArrowColumnarBatch>();

    int numRemaining = numRows;
    Random rand = new Random();
    while (numRemaining > 0) {
      int numRowsInBatch = maxRowsInBatch;
      if (numRowsInBatch > numRemaining) {
        numRowsInBatch = numRemaining;
      }

      List<ValueVector> inputVectors = new ArrayList<>();
      List<ValueVector> outputVectors = new ArrayList<>();
      for (int i = 0; i < dataType.size(); i++) {
        // init input vector
        // IntVector inputVector = new IntVector("input_"+i, allocator);
        BufferAllocator allocator = SparkMemoryUtils.contextAllocator();
        Float4Vector inputVector = new Float4Vector("input_" + i, allocator);
        inputVector.allocateNew(numRowsInBatch);
        for (int j = 0; j < numRowsInBatch; j++) {
          inputVector.setSafe(j, rand.nextFloat());
        }
        inputVectors.add(inputVector);
      }
      inputArrowColumnarBatchs.add(
          new ArrowColumnarBatch(inputVectors, dataType.size(), numRowsInBatch));

      // init output vector
      // IntVector outputVector = new IntVector("result", allocator);
      BufferAllocator allocator = SparkMemoryUtils.contextAllocator();
      Float4Vector outputVector = new Float4Vector("result", allocator);
      outputVector.allocateNew(numRowsInBatch);
      outputVectors.add(outputVector);
      outputArrowColumnarBatchs.add(new ArrowColumnarBatch(outputVectors, 1, numRowsInBatch));

      // fix numRemaining
      numRemaining -= numRowsInBatch;
    }

    /** ***** end preparation ****** */
    try {
      columnarBatchAdd(schema, exprs, inputArrowColumnarBatchs, outputArrowColumnarBatchs);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
