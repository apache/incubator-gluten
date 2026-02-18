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
package org.apache.gluten.execution

import org.apache.spark.internal.Logging

import org.apache.paimon.io.DataFileMeta
import org.apache.paimon.spark.PaimonScan
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.utils.InternalRowPartitionComputer

trait PaimonSparkShim extends Logging {

  /**
   * @return
   *   whether the split is a ChainDataSplit. This class is not available with some spark/paimon
   *   version combinations.
   */
  def isChainSplit(split: DataSplit): Boolean

  /**
   * @param split
   *   input split
   * @return
   *   the partition representation for this particular split
   */
  def getSplitPartition(split: DataSplit): org.apache.paimon.data.InternalRow

  /**
   * @param split
   *   the input split
   * @param file
   *   the file metadata from within the split
   * @return
   *   A string representing the filesystem URI to the bucket of the input file
   */
  def getBucketPath(split: DataSplit, file: DataFileMeta): String

  /**
   * @param scan
   *   the spark scan
   * @return
   *   an implementation of Paimon's {@link InternalRowPartitionComputer}
   */
  def getInternalPartitionComputer(scan: PaimonScan): InternalRowPartitionComputer
}
