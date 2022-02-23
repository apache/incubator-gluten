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

import org.apache.arrow.flatbuf.CompressionType;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DictionaryUtility;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * This class reads from an input stream containing compressed buffers and produces
 * ArrowRecordBatches.
 */
public class SchemaAwareArrowCompressedStreamReader extends ArrowStreamReader {
  public static final String COMPRESS_TYPE_NONE = "none";

  private final Schema originalSchema;

  // fixme: the design can be improved to avoid relying on this stateful field
  private String compressType;

  public SchemaAwareArrowCompressedStreamReader(Schema originalSchema, InputStream in,
      BufferAllocator allocator) {
    super(in, allocator);
    this.originalSchema = originalSchema;
  }


  public SchemaAwareArrowCompressedStreamReader(InputStream in,
      BufferAllocator allocator) {
    this(null, in, allocator);
  }

  public String getCompressType() {
    return compressType;
  }

  @Override
  protected Schema readSchema() throws IOException {
    if (originalSchema == null) {
      return super.readSchema();
    }
    return originalSchema;
  }

  protected void initialize() throws IOException {
    Schema originalSchema = readSchema();
    List<Field> fields = new ArrayList<>();
    List<FieldVector> vectors = new ArrayList<>();
    Map<Long, Dictionary> dictionaries = new HashMap<>();

    // Convert fields with dictionaries to have the index type
    for (Field field : originalSchema.getFields()) {
      Field updated = DictionaryUtility.toMemoryFormat(field, allocator, dictionaries);
      fields.add(updated);
      vectors.add(updated.createVector(allocator));
    }
    Schema schema = new Schema(fields, originalSchema.getCustomMetadata());

    this.root = new VectorSchemaRoot(schema, vectors, 0);
    this.loader = new CompressedVectorLoader(root);
    this.dictionaries = Collections.unmodifiableMap(dictionaries);
  }

  /**
   * Load the next ArrowRecordBatch to the vector schema root if available.
   *
   * @return true if a batch was read, false on EOS
   * @throws IOException on error
   */
  public boolean loadNextBatch() throws IOException {
    prepareLoadNextBatch();
    MessageResult result = messageReader.readNext();

    // Reached EOS
    if (result == null) {
      return false;
    }

    if (result.getMessage().headerType() == MessageHeader.RecordBatch) {
      ArrowBuf bodyBuffer = result.getBodyBuffer();

      // For zero-length batches, need an empty buffer to deserialize the batch
      if (bodyBuffer == null) {
        bodyBuffer = allocator.getEmpty();
      }

      ArrowRecordBatch batch = MessageSerializer.deserializeRecordBatch(result.getMessage(), bodyBuffer);
      byte codec = batch.getBodyCompression().getCodec();
      final String codecName;
      if (codec == NoCompressionCodec.COMPRESSION_TYPE) {
        compressType = COMPRESS_TYPE_NONE;
      } else {
        codecName = CompressionType.name(codec);
        if (codecName.equals("LZ4_FRAME")) {
          compressType = "lz4";
        } else {
          compressType = codecName;
        }
      }

      loadRecordBatch(batch);
      checkDictionaries();
      return true;
    } else if (result.getMessage().headerType() == MessageHeader.DictionaryBatch) {
      // if it's dictionary message, read dictionary message out and continue to read unless get a batch or eos.
      ArrowDictionaryBatch dictionaryBatch = readDictionary(result);
      loadDictionary(dictionaryBatch);
      loadedDictionaryCount++;
      return loadNextBatch();
    } else {
      throw new IOException("Expected RecordBatch or DictionaryBatch but header was " +
        result.getMessage().headerType());
    }
  }

  @Override
  protected void loadRecordBatch(ArrowRecordBatch batch) {
    try {
      CompressedVectorLoader loader = (CompressedVectorLoader) this.loader;
      if (isCurrentBatchCompressed()) {
        loader.loadCompressed(batch);
      } else {
        loader.loadUncompressed(batch);
      }
    } finally {
      batch.close();
    }
  }

  public boolean isCurrentBatchCompressed() {
    return !Objects.equals(getCompressType(), COMPRESS_TYPE_NONE);
  }
}
