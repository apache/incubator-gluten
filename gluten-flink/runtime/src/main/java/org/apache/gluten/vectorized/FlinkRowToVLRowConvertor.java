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

import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.session.Session;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.table.Table;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

/** Converter between velox RowVector and Flink RowData. */
public class FlinkRowToVLRowConvertor {

    public FlinkRowToVLRowConvertor() {
    }

    public static RowVector fromRowData(
            RowData row,
            BufferAllocator allocator,
            Session session) {
        // TODO: finish it. This is just an example now.
        IntVector arrowVector = new IntVector("0", allocator);
        arrowVector.setSafe(0, row.getInt(0));
        return session.arrowOps().fromArrowTable(allocator, Table.of(arrowVector)).asRowVector();
    }

    public static RowData toRowData(
            RowVector rowVector,
            BufferAllocator allocator,
            Session session) {
        FieldVector fieldVector = session.arrowOps().toArrowVector(
                allocator,
                rowVector.loadedVector());
        return GenericRowData.of(((IntVector) fieldVector).get(0));
    }

}
