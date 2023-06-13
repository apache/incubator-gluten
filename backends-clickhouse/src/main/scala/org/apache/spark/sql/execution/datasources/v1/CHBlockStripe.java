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
package org.apache.spark.sql.execution.datasources.v1;

import io.glutenproject.vectorized.CHNativeBlock;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.datasources.BlockStripe;
import org.apache.spark.sql.execution.datasources.CHDatasourceJniWrapper;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class CHBlockStripe extends BlockStripe {

    private UnsafeRow currentHeadingRow;
    private ColumnarBatch currentColumnarBatch;

    public CHBlockStripe(long blockAddress, long headingRowAddress, int headingRowBytes, int bucketId,
                         int rows, int columns) {
        super(blockAddress, headingRowAddress, headingRowBytes, bucketId, rows, columns);
    }

    @Override
    public UnsafeRow getHeadingRow() {
        currentHeadingRow = new UnsafeRow(columns);
        currentHeadingRow.pointTo(null, headingRowAddress, headingRowBytes);
        return currentHeadingRow;
    }

    @Override
    public ColumnarBatch getColumnarBatch() {
        currentColumnarBatch = new CHNativeBlock(blockAddress).toColumnarBatch();
        return currentColumnarBatch;
    }

    @Override
    public void release() {
        CHDatasourceJniWrapper.releaseStripe(blockAddress, headingRowAddress);
    }
}

