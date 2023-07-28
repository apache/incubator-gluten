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

package io.glutenproject.fs;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import io.netty.util.internal.PlatformDependent;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.CopyOption;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;

public class OnHeapFileSystem implements JniFilesystem {

  public static final JniFilesystem INSTANCE = new OnHeapFileSystem();
  private final FileSystem fs;

  private OnHeapFileSystem() {
    fs = Jimfs.newFileSystem(Configuration.unix());
  }

  @Override
  public boolean isCapableForNewFile0(long size) {
    long freeMemory = Runtime.getRuntime().freeMemory();
    return freeMemory > size;
  }

  private void ensureExist(String path) {
    if (!exists(path)) {
      throw new UnsupportedOperationException("OnHeapFileSystem: File doesn't exist: " + path);
    }
  }

  private void ensureNotExist(String path) {
    if (exists(path)) {
      throw new UnsupportedOperationException("OnHeapFileSystem: File already exists " + path);
    }
  }

  @Override
  public ReadFile openFileForRead(String path) {
    ensureExist(path);
    return new ReadFile(fs.getPath(path));
  }

  @Override
  public WriteFile openFileForWrite(String path) {
    ensureNotExist(path);
    return new WriteFile(fs.getPath(path));
  }

  @Override
  public void remove(String path) {
    ensureExist(path);
    try {
      Files.delete(fs.getPath(path));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void rename(String oldPath, String newPath, boolean overwrite) {
    ensureExist(oldPath);
    final CopyOption option;
    if (overwrite) {
      option = StandardCopyOption.REPLACE_EXISTING;
    } else {
      option = StandardCopyOption.ATOMIC_MOVE;
    }
    try {
      Files.move(fs.getPath(oldPath), fs.getPath(newPath), option);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean exists(String path) {
    Path p = fs.getPath(path);
    return Files.exists(p);
  }

  @Override
  public String[] list(String path) {
    ensureExist(path);
    try {
      return Files.list(fs.getPath(path)).map(Path::toString).toArray(String[]::new);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void mkdir(String path) {
    ensureNotExist(path);
    try {
      Files.createDirectory(fs.getPath(path));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void rmdir(String path) {
    ensureExist(path);
    try {
      Files.delete(fs.getPath(path));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static class ReadFile implements JniFilesystem.ReadFile {
    private final Path path;
    private final InputStream in;
    private final AtomicInteger cursor = new AtomicInteger(0);
    private final ReadableByteChannel channel;

    public ReadFile(Path path) {
      this.path = path;
      try {
        in = Files.newInputStream(this.path, StandardOpenOption.READ);
        channel = Channels.newChannel(in);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void pread(long offset, long length, long buf) {
      try {
        ByteBuffer out = PlatformDependent.directBuffer(buf, (int) length);
        if (offset < cursor.get()) {
          throw new IllegalStateException(
              "ReadFile: Offset to read is in front to the cursor position");
        }
        if (offset > cursor.get()) {
          long toSkip = offset - cursor.get();
          long skippedBytes = in.skip(toSkip);
          if (skippedBytes != toSkip) {
            throw new IllegalStateException(
                String.format(
                    "ReadFile: Skipped size mismatch with expected size to skip: %d != %d",
                    skippedBytes, toSkip));
          }
        }
        channel.read(out);
        cursor.getAndAdd((int) length);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean shouldCoalesce() {
      throw new UnsupportedOperationException("Not implemented"); // not used for now
    }

    @Override
    public long size() {
      return cursor.get();
    }

    @Override
    public long memoryUsage() {
      throw new UnsupportedOperationException("Not implemented"); // not used for now
    }

    @Override
    public long getNaturalReadSize() {
      throw new UnsupportedOperationException("Not implemented"); // not used for now
    }

    @Override
    public void close() {
      try {
        channel.close();
        in.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class WriteFile implements JniFilesystem.WriteFile {
    private final Path path;
    private final BufferedOutputStream out;
    private final AtomicInteger writtenBytes = new AtomicInteger(0);

    public WriteFile(Path path) {
      this.path = path;
      try {
        out =
            new BufferedOutputStream(
                Files.newOutputStream(this.path, StandardOpenOption.CREATE_NEW));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void append(byte[] data) {
      try {
        out.write(data);
        writtenBytes.getAndAdd(data.length);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void flush() {
      try {
        out.flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
      try {
        out.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public long size() {
      return writtenBytes.get();
    }
  }
}
