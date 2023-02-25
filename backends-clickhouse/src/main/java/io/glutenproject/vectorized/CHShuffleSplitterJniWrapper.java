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

import java.io.IOException;

public class CHShuffleSplitterJniWrapper {
  public CHShuffleSplitterJniWrapper() {
  }

  public long make(
      NativePartitioning part,
      long mapId,
      int bufferSize,
      String codec,
      String dataFile,
      String localDirs) {
    return nativeMake(
        part.getShortName(),
        part.getNumPartitions(),
        part.getExprList(),
        part.getSchema(),
        mapId,
        bufferSize,
        codec,
        dataFile,
        localDirs);
  }

  public native long nativeMake(
      String shortName,
      int numPartitions,
      byte[] exprList,
      byte[] exprIndexList,
      long mapId,
      int bufferSize,
      String codec,
      String dataFile,
      String localDirs);

  public native void split(long splitterId, int numRows, long block);

  public native SplitResult stop(long splitterId) throws IOException;

  public native void close(long splitterId);
}
