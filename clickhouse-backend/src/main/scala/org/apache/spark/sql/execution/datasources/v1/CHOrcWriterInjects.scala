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
package org.apache.spark.sql.execution.datasources.v1

import org.apache.hadoop.mapreduce.TaskAttemptContext

import java.{util => ju}

class CHOrcWriterInjects extends CHFormatWriterInjects {

  override def nativeConf(
      options: Map[String, String],
      compressionCodec: String): ju.Map[String, String] = {

    // TODO: implement it
    ju.Collections.emptyMap()
  }

  override def createNativeWrite(outputPath: String, context: TaskAttemptContext): Write = Write
    .newBuilder()
    .setCommon(Write.Common.newBuilder().setFormat(formatName).setJobTaskAttemptId("").build())
    .setOrc(Write.OrcWrite.newBuilder().build())
    .build()

  override val formatName: String = "orc"
}
