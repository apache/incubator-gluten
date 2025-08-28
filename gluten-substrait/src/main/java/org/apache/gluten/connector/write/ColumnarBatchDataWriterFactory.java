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
package org.apache.gluten.connector.write;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.Serializable;

/**
 * A factory of {@link DataWriter}, which is responsible for creating and initializing the actual
 * data writer at executor side.
 *
 * <p>Note that, the writer factory will be serialized and sent to executors, then the data writer
 * will be created on executors and do the actual writing. So this interface must be serializable
 * and {@link DataWriter} doesn't need to be.
 *
 * <p>A companion interface with Spark's row bases {@link DataWriterFactory}
 */
@Evolving
public interface ColumnarBatchDataWriterFactory extends Serializable {

  /**
   * Returns a data writer to do the actual writing work. Note that, Spark will reuse the same data
   * object instance when sending data to the data writer, for better performance. Data writers are
   * responsible for defensive copies if necessary, e.g. copy the data before buffer it in a list.
   *
   * <p>If this method fails (by throwing an exception), the corresponding Spark write task would
   * fail and get retried until hitting the maximum retry times.
   */
  DataWriter<ColumnarBatch> createWriter();
}
