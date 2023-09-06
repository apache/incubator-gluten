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
package org.apache.iceberg.spark.source

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.PartitionedFile

import org.apache.iceberg.{FileScanTask, ScanTask}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object GlutenSparkInputPartition {
  def getPartitionedFileArray(inputPartition: InputPartition): Array[PartitionedFile] = {
    val res = new ArrayBuffer[PartitionedFile]()
    inputPartition match {
      case partition: SparkInputPartition =>
        val tasks = partition.taskGroup[ScanTask]().tasks().asScala
        if (tasks.forall(_.isInstanceOf[FileScanTask])) {
          tasks.map(_.asInstanceOf[FileScanTask]).foreach {
            task =>
              val filePath = task.file().path().toString
              val start = task.start()
              val length = task.length()
              res += PartitionedFile(InternalRow.empty, filePath, start, length)
          }
        } else {
          throw new UnsupportedOperationException(s"Only support iceberg FileScanTask.")
        }
      case _ =>
        throw new UnsupportedOperationException(s"Only support iceberg SparkInputPartition.")
    }
    res.toArray
  }

}
