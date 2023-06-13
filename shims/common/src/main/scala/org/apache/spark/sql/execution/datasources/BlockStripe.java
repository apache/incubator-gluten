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
package org.apache.spark.sql.execution.datasources;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/*-
 * BlockStripe is used in writing partitioned/bucketed table.
 * BlockStripe is part of block belonging to same partition/bucket
 */
public class BlockStripe {
    protected long blockAddress;
    protected long headingRowAddress;
    protected int headingRowBytes;
    protected int bucketId;
    protected int rows;
    protected int columns;

    public BlockStripe(long blockAddress, long headingRowAddress, int headingRowBytes, int bucketId, int rows, int columns) {
        this.blockAddress = blockAddress;
        this.headingRowAddress = headingRowAddress;
        this.headingRowBytes = headingRowBytes;
        this.bucketId = bucketId;
        this.rows = rows;
        this.columns = columns;
    }

    public UnsafeRow getHeadingRow() {
        throw new UnsupportedOperationException("subclass of BlockStripe should implement this");
    }

    // get the columnar batch of this block stripe
    public ColumnarBatch getColumnarBatch() {
        throw new UnsupportedOperationException("subclass of BlockStripe should implement this");
    }

    public void release() {
        throw new UnsupportedOperationException("subclass of BlockStripe should implement this");
    }
}

