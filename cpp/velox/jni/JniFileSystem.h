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

#pragma once

#include <jni.h>

#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"

namespace gluten {

// TODO: hongze
struct HeapHandle {
  bool valid() const {
    return true;
  }
};

// TODO: hongze
inline HeapHandle tryAllocHeapMemory(size_t size) {
  HeapHandle handle;
  return handle;
}

// TODO: hongze
inline void freeHeapMemory(HeapHandle handle) {
  // Do something
}

class JniReadFile : public facebook::velox::ReadFile {
 public:
  explicit JniReadFile(HeapHandle handle) : handle_(handle) {}

  std::string_view pread(uint64_t offset, uint64_t length, void* buf) const override;

  bool shouldCoalesce() const override;

  uint64_t size() const override;

  uint64_t memoryUsage() const override;

  std::string getName() const override {
    return "<JniReadFile>";
  }

  uint64_t getNaturalReadSize() const override;

 private:
  HeapHandle handle_;
};

class JniWriteFile : public facebook::velox::WriteFile {
 public:
  explicit JniWriteFile(HeapHandle handle) : handle_(handle) {}

  void append(std::string_view data) override;

  void flush() override;

  void close() override;

  uint64_t size() const override;

 private:
  HeapHandle handle_;
};

class JniFileSystem : public facebook::velox::filesystems::FileSystem {
 public:
  struct FileStub {
    enum { INVALID, HEAP, DISK } type = INVALID;
    HeapHandle handle;
    std::string filename;
  };

  explicit JniFileSystem(std::shared_ptr<const facebook::velox::Config> config) : FileSystem(config) {}

  std::string name() const override {
    return "JNI FS";
  }

  std::unique_ptr<facebook::velox::ReadFile> openFileForRead(
      std::string_view path,
      const facebook::velox::filesystems::FileOptions& options) override;

  std::unique_ptr<facebook::velox::WriteFile> openFileForWrite(
      std::string_view path,
      const facebook::velox::filesystems::FileOptions& options) override;

  void remove(std::string_view path) override;

  void rename(std::string_view oldPath, std::string_view newPath, bool overwrite) override;

  bool exists(std::string_view path) override;

  std::vector<std::string> list(std::string_view path) override;

  void mkdir(std::string_view path) override;

  void rmdir(std::string_view path) override;

  static std::function<bool(std::string_view)> schemeMatcher();

  static std::function<std::shared_ptr<FileSystem>(std::shared_ptr<const facebook::velox::Config>, std::string_view)>
  fileSystemGenerator();

 private:
  std::map<std::string /*path*/, FileStub> fileStubs_;
  std::mutex fileStubsMutex_;
};

void registerJniFileSystem();

void initVeloxJniFileSystem(JNIEnv* env);

void finalizeVeloxJniFileSystem(JNIEnv* env);

} // namespace gluten
