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

static jclass jniFileSystemClass;
static jclass jniReadFileClass;
static jclass jniWriteFileClass;


static jmethodID jniFileSystemOpenFileForRead;
static jmethodID jniFileSystemOpenFileForWrite;
static jmethodID jniFileSystemRemove;
static jmethodID jniFileSystemRename;
static jmethodID jniFileSystemExists;
static jmethodID jniFileSystemList;
static jmethodID jniFileSystemMkdir;
static jmethodID jniFileSystemRmdir;

static jmethodID jniReadFilePread;
static jmethodID jniReadFileShouldCoalesce;
static jmethodID jniReadFileSize;
static jmethodID jniReadFileMemoryUsage;
static jmethodID jniReadFileGetNaturalReadSize;

static jmethodID jniWriteFileAppend;
static jmethodID jniWriteFileFlush;
static jmethodID jniWriteFileClose;
static jmethodID jniWriteFileSize;

}

void gluten::initVeloxJniFileSystem(JNIEnv* env) {
  // classes
  jniFileSystemClass =
      createGlobalClassReferenceOrError(env, "Lio/glutenproject/fs/JniFilesystem;");
  jniReadFileClass =
      createGlobalClassReferenceOrError(env, "Lio/glutenproject/fs/JniFilesystem$ReadFile;");
  jniWriteFileClass =
      createGlobalClassReferenceOrError(env, "Lio/glutenproject/fs/JniFilesystem$WriteFile;");

  // methods in JniFilesystem
  jniFileSystemOpenFileForRead = getMethodIdOrError(env, jniFileSystemClass, "openFileForRead", "(Ljava/lang/String;)Lio/glutenproject/fs/JniFilesystem$ReadFile;");
  jniFileSystemOpenFileForWrite = getMethodIdOrError(env, jniFileSystemClass, "openFileForWrite", "(Ljava/lang/String;)Lio/glutenproject/fs/JniFilesystem$WriteFile;");
  jniFileSystemRemove = getMethodIdOrError(env, jniFileSystemClass, "remove", "(Ljava/lang/String;)Z");
  jniFileSystemRename = getMethodIdOrError(env, jniFileSystemClass, "rename", "(Ljava/lang/String;Ljava/lang/String;Z)Z");
  jniFileSystemExists = getMethodIdOrError(env, jniFileSystemClass, "exists", "(Ljava/lang/String;)Z");
  jniFileSystemList = getMethodIdOrError(env, jniFileSystemClass, "list", "(Ljava/lang/String;)[Ljava/lang/String;");
  jniFileSystemMkdir = getMethodIdOrError(env, jniFileSystemClass, "list", "(Ljava/lang/String;)V");
  jniFileSystemRmdir = getMethodIdOrError(env, jniFileSystemClass, "list", "(Ljava/lang/String;)V");

  // methods in JniFilesystem$ReadFile
  jniReadFilePread = getMethodIdOrError(env, jniReadFileClass, "pread", "(JJJ)[B");
  jniReadFileShouldCoalesce = getMethodIdOrError(env, jniReadFileClass, "shouldCoalesce", "()Z");
  jniReadFileSize = getMethodIdOrError(env, jniReadFileClass, "size", "()J");
  jniReadFileMemoryUsage = getMethodIdOrError(env, jniReadFileClass, "memoryUsage", "()J");
  jniReadFileGetNaturalReadSize = getMethodIdOrError(env, jniReadFileClass, "getNaturalReadSize", "()J");

  // methods in JniFilesystem$WriteFile
  jniWriteFileAppend = getMethodIdOrError(env, jniWriteFileClass, "append", "([B)V");
  jniWriteFileFlush = getMethodIdOrError(env, jniWriteFileClass, "flush", "()V");
  jniWriteFileClose = getMethodIdOrError(env, jniWriteFileClass, "close", "()V");
  jniWriteFileSize = getMethodIdOrError(env, jniWriteFileClass, "size", "()V");
}

void gluten::finalizeVeloxJniFileSystem(JNIEnv* env) {
    env->DeleteGlobalRef(jniWriteFileClass);
    env->DeleteGlobalRef(jniReadFileClass);
    env->DeleteGlobalRef(jniFileSystemClass);
}

void gluten::registerJniFileSystem() {
  facebook::velox::filesystems::registerFileSystem(
      JniFileSystem::schemeMatcher(), JniFileSystem::fileSystemGenerator());
}

std::string_view gluten::JniReadFile::pread(uint64_t offset, uint64_t length, void* buf) const {
  return std::string_view();
}

bool gluten::JniReadFile::shouldCoalesce() const {
  return false;
}

uint64_t gluten::JniReadFile::size() const {
  return 0;
}

uint64_t gluten::JniReadFile::memoryUsage() const {
  return 0;
}

std::string gluten::JniReadFile::getName() const {
  return std::string();
}

uint64_t gluten::JniReadFile::getNaturalReadSize() const {
  return 0;
}

void gluten::JniWriteFile::append(std::string_view data) {}

void gluten::JniWriteFile::flush() {}

void gluten::JniWriteFile::close() {}

uint64_t gluten::JniWriteFile::size() const {
  return 0;
}

gluten::JniFileSystem::JniFileSystem(std::shared_ptr<const facebook::velox::Config> config) : FileSystem(config) {}

std::string gluten::JniFileSystem::name() const {
  return "JNI FS";
}

std::unique_ptr<facebook::velox::ReadFile> gluten::JniFileSystem::openFileForRead(
    std::string_view path,
    const facebook::velox::filesystems::FileOptions& options) {
  return std::unique_ptr<facebook::velox::ReadFile>();
}

std::unique_ptr<facebook::velox::WriteFile> gluten::JniFileSystem::openFileForWrite(
    std::string_view path,
    const facebook::velox::filesystems::FileOptions& options) {
  return std::unique_ptr<facebook::velox::WriteFile>();
}

void gluten::JniFileSystem::remove(std::string_view path) {}

void gluten::JniFileSystem::rename(std::string_view oldPath, std::string_view newPath, bool overwrite) {}

bool gluten::JniFileSystem::exists(std::string_view path) {
  return false;
}

std::vector<std::string> gluten::JniFileSystem::list(std::string_view path) {
  return std::vector<std::string>();
}

void gluten::JniFileSystem::mkdir(std::string_view path) {}

void gluten::JniFileSystem::rmdir(std::string_view path) {}

std::function<bool(std::string_view)> gluten::JniFileSystem::schemeMatcher() {
  return [](std::string_view filePath) { return filePath.find(kFileScheme) == 0; };
}

std::function<std::shared_ptr<
    facebook::velox::filesystems::FileSystem>(std::shared_ptr<const facebook::velox::Config>, std::string_view)>
gluten::JniFileSystem::fileSystemGenerator() {
  return [](std::shared_ptr<const facebook::velox::Config> properties, std::string_view filePath) {
    static std::shared_ptr<FileSystem> lfs = std::make_shared<JniFileSystem>(properties);
    return lfs;
  };
}
