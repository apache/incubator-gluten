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
package org.apache.gluten.vectorized;

import io.github.zhztheplayer.velox4j.arrow.Arrow;
import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.table.Table;

import java.util.ArrayList;
import java.util.List;

/** Converter between velox RowVector and Flink RowData. */
public class FlinkRowToVLVectorConvertor {
  public static RowVector fromRowData(
      RowData row, BufferAllocator allocator, Session session, RowType rowType) {
    List<FieldVector> arrowVectors = new ArrayList<>(rowType.size());
    List<Type> fieldTypes = rowType.getChildren();
    List<String> fieldNames = rowType.getNames();
    for (int i = 0; i < rowType.size(); i++) {
      Type fieldType = rowType.getChildren().get(i);
      ArrowVectorWriter writer =
          ArrowVectorWriter.create(fieldNames.get(i), fieldTypes.get(i), allocator);
      writer.write(i, row);
      writer.finish();
      arrowVectors.add(i, writer.getVector());
    }

    return session.arrowOps().fromArrowTable(allocator, new Table(arrowVectors));
  }

  public static List<RowData> toRowData(
      RowVector rowVector, BufferAllocator allocator, RowType rowType) {
    // TODO: support more types
    BaseVector loadedVector = null;
    FieldVector structVector = null;

    try {
      loadedVector = rowVector.loadedVector();
      // The result is StructVector
      structVector = Arrow.toArrowVector(allocator, loadedVector);
      final List<FieldVector> fieldVectors = structVector.getChildrenFromFields();
      List<ArrowVectorAccessor> accessors = buildArrowVectorAccessors(fieldVectors);
      List<RowData> rowDatas = new ArrayList<>(rowVector.getSize());
      for (int j = 0; j < rowVector.getSize(); j++) {
        Object[] fieldValues = new Object[rowType.size()];
        for (int i = 0; i < rowType.size(); i++) {
          fieldValues[i] = accessors.get(i).get(j);
        }
        rowDatas.add(GenericRowData.of(fieldValues));
      }
      return rowDatas;
    } finally {
      /// The FieldVector/BaseVector should be closed in `finally`, to avoid it may not be closed
      // when exceptions rasied,
      /// that lead to memory leak.
      if (structVector != null) {
        structVector.close();
      }
      if (loadedVector != null) {
        loadedVector.close();
      }
    }
  }

  private static List<ArrowVectorAccessor> buildArrowVectorAccessors(List<FieldVector> vectors) {
    List<ArrowVectorAccessor> accessors = new ArrayList<>(vectors.size());
    for (int i = 0; i < vectors.size(); ++i) {
      accessors.add(i, ArrowVectorAccessor.create(vectors.get(i)));
    }
    return accessors;
  }
}
