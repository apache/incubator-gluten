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

#include "JniFileSystem.h"
#include "jni/JniCommon.h"
#include "jni/JniErrors.h"

namespace {

constexpr std::string_view kFileScheme("jni:");

static JavaVM* vm;

jstring createJString(JNIEnv* env, const std::string_view& path) {
  return env->NewStringUTF(std::string(path).c_str());
}

std::shared_ptr<facebook::velox::filesystems::FileSystem> getLocalFileSystem() {
  return facebook::velox::filesystems::getFileSystem("file:", nullptr);
}

// TODO: determine the value according to spilling code
size_t getFileSizeLimit(const facebook::velox::filesystems::FileOptions& options) {
  auto x = options.values.find("fileSizeLimit");
  if (x == options.values.end()) {
    return (1 << 20); // 1 Mega Bytes
  }
  return std::atoi(x->second.c_str());
}

} // namespace

namespace gluten {

void initVeloxJniFileSystem(JNIEnv* env) {
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw GlutenException("Unable to get JavaVM instance");
  }
}

void finalizeVeloxJniFileSystem(JNIEnv* env) {
  vm = nullptr;
}

std::string_view JniReadFile::pread(uint64_t offset, uint64_t length, void* buf) const {
  GLUTEN_CHECK(false, "JniWriteFile::pread TODO");
  return std::string_view(reinterpret_cast<const char*>(buf));
}

bool JniReadFile::shouldCoalesce() const {
  GLUTEN_CHECK(false, "JniWriteFile::shouldCoalesce TODO");
  return false;
}

uint64_t JniReadFile::size() const {
  GLUTEN_CHECK(false, "JniWriteFile::size TODO");
  return 0;
}

uint64_t JniReadFile::memoryUsage() const {
  GLUTEN_CHECK(false, "JniWriteFile::memoryUsage TODO");
  return 0;
}

uint64_t JniReadFile::getNaturalReadSize() const {
  GLUTEN_CHECK(false, "JniWriteFile::getNaturalReadSize TODO");
  return 0;
}

void JniWriteFile::append(std::string_view data) {
  GLUTEN_CHECK(false, "JniWriteFile::append TODO");
}

void JniWriteFile::flush() {
  GLUTEN_CHECK(false, "JniWriteFile::flush TODO");
}

void JniWriteFile::close() {
  GLUTEN_CHECK(false, "JniWriteFile::close TODO");
}

uint64_t JniWriteFile::size() const {
  GLUTEN_CHECK(false, "JniWriteFile::size TODO");
  return 0;
}

std::unique_ptr<facebook::velox::ReadFile> JniFileSystem::openFileForRead(
    std::string_view path,
    const facebook::velox::filesystems::FileOptions& options) {
  // path = "jni:/spillDir/pipelineId_driverId_operatorId-fileNo-ordinal";
  // spillDir is set by task_->setSpillDirectory(spillDir) in the constructor of WholeStageResultIteratorFirstStage

  GLUTEN_CHECK(
      !options.values.empty(),
      "JniFileSystem::openFileForRead: file options is not empty, this is not currently supported");

  // get stub
  FileStub stub;
  {
    std::lock_guard<std::mutex> lg(fileStubsMutex_);
    auto x = fileStubs_.find(std::string(path));
    GLUTEN_CHECK(x != fileStubs_.end(), "JniFileSystem::openFileForRead not found stub");
    stub = x->second;
  }

  std::unique_ptr<facebook::velox::ReadFile> readFile;
  if (stub.type == FileStub::HEAP) {
    readFile = std::make_unique<JniReadFile>(stub.handle);
  } else {
    auto filename = path.substr(kFileScheme.size()); // skip 'jni:'
    readFile = getLocalFileSystem()->openFileForRead(filename);
  }

  return readFile;
}

std::unique_ptr<facebook::velox::WriteFile> JniFileSystem::openFileForWrite(
    std::string_view path,
    const facebook::velox::filesystems::FileOptions& options) {
  // path = "jni:/spillDir/pipelineId_driverId_operatorId-fileNo-ordinal";
  // I think the spillDir is an unique string, need to confirm
  FileStub stub;
  std::unique_ptr<facebook::velox::WriteFile> writeFile;

  size_t size = getFileSizeLimit(options);
  auto handle = tryAllocHeapMemory(size);
  if (handle.valid()) {
    writeFile = std::make_unique<JniWriteFile>(handle);
    stub.type = FileStub::HEAP;
    stub.handle = handle;
  } else {
    auto filename = path.substr(kFileScheme.size()); // skip 'jni:'
    writeFile = getLocalFileSystem()->openFileForWrite(filename);
    stub.type = FileStub::DISK;
    stub.filename = filename;
  }

  // add stub
  {
    std::lock_guard<std::mutex> lg(fileStubsMutex_);
    auto result = fileStubs_.insert(std::make_pair(std::string(path), stub));
    GLUTEN_CHECK(result.second, "JniFileSystem::openFileForWrite insert stub failed");
  }

  return writeFile;
}

void JniFileSystem::remove(std::string_view path) {
  GLUTEN_CHECK(false, "JniFileSystem::remove TODO");
}

void JniFileSystem::rename(std::string_view oldPath, std::string_view newPath, bool overwrite) {
  GLUTEN_CHECK(false, "JniFileSystem::rename TODO");
}

bool JniFileSystem::exists(std::string_view path) {
  GLUTEN_CHECK(false, "JniFileSystem::exists TODO");
  return false;
}

std::vector<std::string> JniFileSystem::list(std::string_view path) {
  GLUTEN_CHECK(false, "JniFileSystem::list TODO");
  std::vector<std::string> out;
  return out;
}

void JniFileSystem::mkdir(std::string_view path) {
  GLUTEN_CHECK(false, "JniFileSystem::mkdir TODO");
}

void JniFileSystem::rmdir(std::string_view path) {
  // path = "jni:/spillDir"
  // rethink: does path need to be transferred

  // step1: remove files under spill dir
  getLocalFileSystem()->rmdir(path);

  // step2: free heap memories
  {
    std::lock_guard<std::mutex> lg(fileStubsMutex_);
    for (auto x = fileStubs_.begin(); x != fileStubs_.end();) {
      if (x->first.find_first_of(path) == 0) {
        if (x->second.type == FileStub::HEAP) {
          freeHeapMemory(x->second.handle);
        }
        x = fileStubs_.erase(x);
      } else {
        ++x;
      }
    }
  }
}

std::function<bool(std::string_view)> JniFileSystem::schemeMatcher() {
  return [](std::string_view filePath) { return filePath.find(kFileScheme) == 0; };
}

std::function<std::shared_ptr<
    facebook::velox::filesystems::FileSystem>(std::shared_ptr<const facebook::velox::Config>, std::string_view)>
JniFileSystem::fileSystemGenerator() {
  return [](std::shared_ptr<const facebook::velox::Config> properties, std::string_view filePath) {
    return std::make_shared<JniFileSystem>(properties);
  };
}

void registerJniFileSystem() {
  facebook::velox::filesystems::registerFileSystem(
      JniFileSystem::schemeMatcher(), JniFileSystem::fileSystemGenerator());
}

} // namespace gluten
