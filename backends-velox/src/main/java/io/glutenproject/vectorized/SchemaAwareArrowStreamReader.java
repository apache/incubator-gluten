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

package io.glutenproject.vectorized;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DictionaryUtility;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class SchemaAwareArrowStreamReader extends ArrowStreamReader {
  private final Schema originalSchema;
  private CompressType compressType;

  public SchemaAwareArrowStreamReader(Schema originalSchema, InputStream in,
                                      BufferAllocator allocator) {
    super(in, allocator);
    this.originalSchema = originalSchema;
  }

  @Override
  protected Schema readSchema() throws IOException {
    if (originalSchema == null) {
      return super.readSchema();
    }
    return originalSchema;
  }

  @Override
  protected void initialize() throws IOException {
    Schema originalSchema = this.readSchema();
    List<Field> fields = new ArrayList(originalSchema.getFields().size());
    List<FieldVector> vectors = new ArrayList(originalSchema.getFields().size());
    Map<Long, Dictionary> dictionaries = new HashMap();
    Iterator var5 = originalSchema.getFields().iterator();

    while(var5.hasNext()) {
      Field field = (Field)var5.next();
      Field updated = DictionaryUtility.toMemoryFormat(field, this.allocator, dictionaries);
      fields.add(updated);
      vectors.add(updated.createVector(this.allocator));
    }

    Schema schema = new Schema(fields, originalSchema.getCustomMetadata());
    this.root = new VectorSchemaRoot(schema, vectors, 0) {
      @Override
      public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
      }
    };
    this.loader = new VectorLoader(this.root);
    this.dictionaries = Collections.unmodifiableMap(dictionaries);
  }


  @Override
  protected void loadRecordBatch(ArrowRecordBatch batch) {
    compressType = CompressType.fromCompressionType(batch.getBodyCompression().getCodec());
    super.loadRecordBatch(batch);
  }


  public CompressType getBatchCompressType() {
    return compressType;
  }
}
