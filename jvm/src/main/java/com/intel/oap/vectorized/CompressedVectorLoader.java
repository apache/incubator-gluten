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

package com.intel.oap.vectorized;

import java.util.Iterator;

import org.apache.arrow.util.Collections2;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import org.apache.arrow.memory.ArrowBuf;

/** Loads compressed buffers into vectors. */
public class CompressedVectorLoader extends VectorLoader {

  /**
   * Construct with a root to load and will create children in root based on schema.
   *
   * @param root the root to add vectors to based on schema
   */
  public CompressedVectorLoader(VectorSchemaRoot root) {
    super(root);
  }

  /**
   * Loads the record batch in the vectors. will not close the record batch
   *
   * @param recordBatch the batch to load
   */
  public void loadCompressed(ArrowRecordBatch recordBatch) {
    Iterator<ArrowBuf> buffers = recordBatch.getBuffers().iterator();
    Iterator<ArrowFieldNode> nodes = recordBatch.getNodes().iterator();
    for (FieldVector fieldVector : root.getFieldVectors()) {
      // When support Fastpfor algorithm, we can not use the existing decompress
      // implementation in arrow 3.0 java side. Because we only use the Fastpfor to
      // compress the int data type and the other data types still use the LZ4 algorithm in the record batch.
      // So here we remove the codec parameter when call the loadBuffers() method in VectorLoader.
      // And the decompress function is done in ShuffleDecompressionJniWrapper#decompress() method.
      loadBuffers(fieldVector, fieldVector.getField(), buffers, nodes);
    }
    root.setRootRowCount(recordBatch.getLength());
    if (nodes.hasNext() || buffers.hasNext()) {
      throw new IllegalArgumentException(
          "not all nodes and buffers were consumed. nodes: "
              + Collections2.toList(nodes).toString()
              + " buffers: "
              + Collections2.toList(buffers).toString());
    }
  }

  /**
   * Direct router to VectorLoader#load()
   */
  public void loadUncompressed(ArrowRecordBatch recordBatch) {
    super.load(recordBatch);
  }
}
