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

import java.io.*;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.arrow.util.AutoCloseables;

/** ArrowBufBuilder. */
public class SerializableObject implements Externalizable, KryoSerializable {
  public int total_size;
  public int[] size;
  private ByteBuf[] directAddrs;

  private transient AutoCloseable[] resources = null;

  public SerializableObject() {}

  /**
   * Create an instance for NativeSerializableObject.
   *
   * @param memoryAddress native ArrowBuf data addr.
   * @param size ArrowBuf size.
   */
  public SerializableObject(long[] memoryAddress, int[] size, AutoCloseable[] resources) throws IOException {
    this.total_size = 0;
    this.size = size;
    directAddrs = new ByteBuf[size.length];
    for (int i = 0; i < size.length; i++) {
      this.total_size += size[i];
      directAddrs[i] = Unpooled.wrappedBuffer(memoryAddress[i], size[i], false);
    }
    this.resources = resources;
  }

  public SerializableObject(NativeSerializableObject obj, AutoCloseable[] resources)
      throws IOException, ClassNotFoundException {
    this(obj.memoryAddress, obj.size, resources);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.total_size = in.readInt();
    int size_len = in.readInt();
    this.size = (int[]) in.readObject();
    ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
    directAddrs = new ByteBuf[size_len];
    for (int i = 0; i < size.length; i++) {
      directAddrs[i] = allocator.directBuffer(size[i], size[i]);
      if (size[i] > 0) {
        byte[] data = new byte[size[i]];
        data = (byte[]) in.readObject();
        OutputStream out = new ByteBufOutputStream(directAddrs[i]);
        out.write(data);
        out.close();
      }
    }
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(this.total_size);
    out.writeInt(this.size.length);
    out.writeObject(this.size);
    for (int i = 0; i < size.length; i++) {
      if (size[i] > 0) {
        byte[] data = new byte[size[i]];
        ByteBufInputStream in = new ByteBufInputStream(directAddrs[i]);
        in.read(data);
        out.writeObject(data);
      }
    }
  }

  @Override
  public void read(Kryo kryo, Input in) {
    this.total_size = in.readInt();
    int size_len = in.readInt();
    this.size = in.readInts(size_len);
    ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
    directAddrs = new ByteBuf[size_len];
    for (int i = 0; i < size.length; i++) {
      directAddrs[i] = allocator.directBuffer(size[i], size[i]);
      if (size[i] > 0) {
        byte[] data = new byte[size[i]];
        OutputStream out = new ByteBufOutputStream(directAddrs[i]);
        try {
          in.readBytes(data);
          out.write(data);
          out.close();
        } catch (IOException e) {
        }
      }
    }
  }

  @Override
  public void write(Kryo kryo, Output out) {
    out.writeInt(this.total_size);
    out.writeInt(this.size.length);
    out.writeInts(this.size);
    for (int i = 0; i < size.length; i++) {
      if (size[i] > 0) {
        byte[] data = new byte[size[i]];
        ByteBufInputStream in = new ByteBufInputStream(directAddrs[i]);
        try {
          in.read(data);
        } catch (IOException e) {
        }
        out.writeBytes(data);
      }
    }
  }

  public void close() {
    releaseDirectMemory();
    releaseResources();
  }

  public long[] getDirectMemoryAddrs() throws IOException {
    if (directAddrs == null) {
      throw new IOException("DirectAddrs is null");
    }
    long[] addrs = new long[size.length];
    for (int i = 0; i < size.length; i++) {
      addrs[i] = directAddrs[i].memoryAddress();
    }
    return addrs;
  }

  private void releaseDirectMemory() {
    if (directAddrs != null) {
      for (int i = 0; i < directAddrs.length; i++) {
        directAddrs[i].release();
      }
    }
  }

  private void releaseResources() {
    if (resources == null) {
      return;
    }
    try {
      AutoCloseables.close(resources);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}