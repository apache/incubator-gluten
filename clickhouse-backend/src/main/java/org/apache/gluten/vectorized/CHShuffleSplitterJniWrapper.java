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

import org.apache.gluten.execution.ColumnarNativeIterator;

import java.io.IOException;

public class CHShuffleSplitterJniWrapper {
  public CHShuffleSplitterJniWrapper() {}

  public long make(
      ColumnarNativeIterator records,
      NativePartitioning part,
      int shuffleId,
      long mapId,
      int bufferSize,
      String codec,
      int level,
      String dataFile,
      String localDirs,
      int subDirsPerLocalDir,
      long spillThreshold,
      String hashAlgorithm,
      long maxSortBufferSize,
      boolean forceMemorySort) {
    return nativeMake(
        records,
        part.getShortName(),
        part.getNumPartitions(),
        part.getExprList(),
        part.getRequiredFields(),
        shuffleId,
        mapId,
        bufferSize,
        codec,
        level,
        dataFile,
        localDirs,
        subDirsPerLocalDir,
        spillThreshold,
        hashAlgorithm,
        maxSortBufferSize,
        forceMemorySort);
  }

  public long makeForRSS(
      ColumnarNativeIterator records,
      NativePartitioning part,
      int shuffleId,
      long mapId,
      int bufferSize,
      String codec,
      int level,
      long spillThreshold,
      String hashAlgorithm,
      Object pusher,
      boolean forceMemorySort) {
    return nativeMakeForRSS(
        records,
        part.getShortName(),
        part.getNumPartitions(),
        part.getExprList(),
        part.getRequiredFields(),
        shuffleId,
        mapId,
        bufferSize,
        codec,
        level,
        spillThreshold,
        hashAlgorithm,
        pusher,
        forceMemorySort);
  }

  public native long nativeMake(
      ColumnarNativeIterator records,
      String shortName,
      int numPartitions,
      byte[] exprList,
      byte[] exprIndexList,
      int shuffleId,
      long mapId,
      int bufferSize,
      String codec,
      int level,
      String dataFile,
      String localDirs,
      int subDirsPerLocalDir,
      long spillThreshold,
      String hashAlgorithm,
      long maxSortBufferSize,
      boolean forceMemorySort);

  public native long nativeMakeForRSS(
      ColumnarNativeIterator records,
      String shortName,
      int numPartitions,
      byte[] exprList,
      byte[] exprIndexList,
      int shuffleId,
      long mapId,
      int bufferSize,
      String codec,
      int level,
      long spillThreshold,
      String hashAlgorithm,
      Object pusher,
      boolean forceMemorySort);

  public native CHSplitResult stop(long splitterId) throws IOException;

  public native void close(long splitterId);
}
