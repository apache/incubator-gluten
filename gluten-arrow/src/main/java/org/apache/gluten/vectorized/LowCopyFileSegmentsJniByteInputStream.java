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

import org.apache.gluten.exception.GlutenException;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * This implementation is targeted to optimize against Spark's {@link
 * org.apache.spark.network.buffer.FileSegmentManagedBuffer} to make sure shuffle data is shared
 * over JNI without unnecessary copy.
 */
public class LowCopyFileSegmentsJniByteInputStream implements JniByteInputStream {
  private static final Field FIELD_SequenceInputStream_in;
  private static final Field FIELD_SequenceInputStream_e;

  static {
    try {
      FIELD_SequenceInputStream_in = SequenceInputStream.class.getDeclaredField("in");
      FIELD_SequenceInputStream_in.setAccessible(true);
      FIELD_SequenceInputStream_e = SequenceInputStream.class.getDeclaredField("e");
      FIELD_SequenceInputStream_e.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new GlutenException(e);
    }
  }

  private final InputStream in;
  private final List<LowCopyFileSegmentJniByteInputStream> segments;

  private int currentIndex = 0;
  private long bytesRead = 0L;

  public LowCopyFileSegmentsJniByteInputStream(InputStream in) {
    this.in = in; // to prevent underlying netty buffer from being collected by GC
    final InputStream unwrapped = JniByteInputStreams.unwrapSparkInputStream(in);
    final SequenceInputStream sin = (SequenceInputStream) unwrapped;
    final List<InputStream> streams = collectStreams(sin, false);
    this.segments = buildSegments(streams);
  }

  public static boolean isSupported(InputStream in) {
    if (!(in instanceof SequenceInputStream)) {
      return false;
    }
    final SequenceInputStream sin = (SequenceInputStream) in;
    final List<InputStream> streams = collectStreams(sin, true);
    for (InputStream stream : streams) {
      final InputStream unwrapped = JniByteInputStreams.unwrapSparkInputStream(stream);
      if (!LowCopyFileSegmentJniByteInputStream.isSupported(unwrapped)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public long read(long destAddress, long maxSize) {
    if (maxSize <= 0) {
      return 0;
    }
    long remaining = maxSize;
    long totalRead = 0L;
    while (remaining > 0 && currentIndex < segments.size()) {
      LowCopyFileSegmentJniByteInputStream segment = segments.get(currentIndex);
      long request = remaining;
      long bytes = segment.read(destAddress + totalRead, request);
      if (bytes == 0) {
        currentIndex++;
        continue;
      }
      bytesRead += bytes;
      remaining -= bytes;
      totalRead += bytes;
    }
    return totalRead;
  }

  @Override
  public long tell() {
    return bytesRead;
  }

  @Override
  public void close() {
    try {
      for (LowCopyFileSegmentJniByteInputStream segment : segments) {
        segment.close();
      }
      in.close();
    } catch (IOException e) {
      throw new GlutenException(e);
    }
  }

  private static List<LowCopyFileSegmentJniByteInputStream> buildSegments(
      List<InputStream> streams) {
    List<LowCopyFileSegmentJniByteInputStream> segments = new ArrayList<>(streams.size());
    for (InputStream stream : streams) {
      segments.add(new LowCopyFileSegmentJniByteInputStream(stream));
    }
    return segments;
  }

  private static List<InputStream> collectStreams(SequenceInputStream sin, boolean restore) {
    final List<InputStream> streams = new ArrayList<>();
    final InputStream current;
    final Enumeration<? extends InputStream> enumeration;
    try {
      current = (InputStream) FIELD_SequenceInputStream_in.get(sin);
      enumeration = (Enumeration<? extends InputStream>) FIELD_SequenceInputStream_e.get(sin);
    } catch (IllegalAccessException e) {
      throw new GlutenException(e);
    }
    if (current != null) {
      streams.add(current);
    }
    if (enumeration != null) {
      while (enumeration.hasMoreElements()) {
        streams.add(enumeration.nextElement());
      }
    }

    if (restore) {
      // restore the enumeration in SequenceInputStream.e
      try {
        if (streams.isEmpty()) {
          FIELD_SequenceInputStream_e.set(sin, Collections.enumeration(Collections.emptyList()));
        } else {
          if (streams.size() == 1) {
            FIELD_SequenceInputStream_e.set(sin, Collections.enumeration(Collections.emptyList()));
          } else {
            FIELD_SequenceInputStream_e.set(
                sin, Collections.enumeration(streams.subList(1, streams.size())));
          }
        }
      } catch (IllegalAccessException e) {
        throw new GlutenException(e);
      }
    }
    return streams;
  }
}
