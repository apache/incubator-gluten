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

import org.apache.spark.storage.BufferReleasingInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.zip.CheckedInputStream;

/**
 * Create optimal {@link JniByteInputStream} implementation from Java {@link InputStream}.
 */
public final class JniInputStreams {
  private static final Logger LOG =
      LoggerFactory.getLogger(JniInputStreams.class);


  private static final Field FIELD_FilterInputStream_in;

  static {
    try {
      FIELD_FilterInputStream_in = FilterInputStream.class.getDeclaredField("in");
      FIELD_FilterInputStream_in.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  private JniInputStreams() {
  }

  public static JniByteInputStream create(InputStream in) {
    // Unwrap BufferReleasingInputStream
    final InputStream unwrapped = unwrapSparkInputStream(in);
    LOG.info("InputStream is of class " + unwrapped.getClass().getName());
    if (LowCopyNettyJniByteInputStream.isSupported(unwrapped)) {
      LOG.info("Creating LowCopyNettyJniByteInputStream");
      return new LowCopyNettyJniByteInputStream(in);
    }
    LOG.info("Creating OnHeapJniByteInputStream");
    return new OnHeapJniByteInputStream(in);
  }

  static InputStream unwrapSparkInputStream(InputStream in) {
    InputStream unwrapped = in;
    if (in instanceof BufferReleasingInputStream) {
      final BufferReleasingInputStream brin = (BufferReleasingInputStream) in;
      unwrapped = org.apache.spark.storage.OASPackageBridge.unwrapBufferReleasingInputStream(brin);
    }
    if (in instanceof CheckedInputStream) {
      final CheckedInputStream cin = (CheckedInputStream) in;
      try {
        unwrapped = ((InputStream) FIELD_FilterInputStream_in.get(cin));
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    return unwrapped;
  }
}
