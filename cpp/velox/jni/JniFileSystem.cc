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

namespace {
constexpr std::string_view kJniFsScheme("jni:");
constexpr std::string_view kJolFsScheme("jol:");

JavaVM* vm;

jclass jniFileSystemClass;
jclass jniReadFileClass;
jclass jniWriteFileClass;

jmethodID jniGetFileSystem;
jmethodID jniIsCapableForNewFile;
jmethodID jniFileSystemOpenFileForRead;
jmethodID jniFileSystemOpenFileForWrite;
jmethodID jniFileSystemRemove;
jmethodID jniFileSystemRename;
jmethodID jniFileSystemExists;
jmethodID jniFileSystemList;
jmethodID jniFileSystemMkdir;
jmethodID jniFileSystemRmdir;

jmethodID jniReadFilePread;
jmethodID jniReadFileShouldCoalesce;
jmethodID jniReadFileSize;
jmethodID jniReadFileMemoryUsage;
jmethodID jniReadFileGetNaturalReadSize;

jmethodID jniWriteFileAppend;
jmethodID jniWriteFileFlush;
jmethodID jniWriteFileClose;
jmethodID jniWriteFileSize;

jstring createJString(JNIEnv* env, const std::string_view& path) {
  return env->NewStringUTF(std::string(path).c_str());
}

std::string_view removePathProtocol(std::string_view path) {
  unsigned long pos = path.find(':');
  if (pos == std::string::npos) {
    return path;
  }
  return path.substr(pos + 1);
}

class JniReadFile : public facebook::velox::ReadFile {
 public:
  explicit JniReadFile(jobject obj) {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    obj_ = env->NewGlobalRef(obj);
    checkException(env);
  }

  ~JniReadFile() override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    env->DeleteGlobalRef(obj_);
    checkException(env);
  }

  std::string_view pread(uint64_t offset, uint64_t length, void* buf) const override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    env->CallVoidMethod(
        obj_, jniReadFilePread, static_cast<jlong>(offset), static_cast<jlong>(length), reinterpret_cast<jlong>(buf));
    checkException(env);
    return std::string_view(reinterpret_cast<const char*>(buf));
  }

  bool shouldCoalesce() const override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    jboolean out = env->CallBooleanMethod(obj_, jniReadFileShouldCoalesce);
    checkException(env);
    return out;
  }

  uint64_t size() const override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    jlong out = env->CallLongMethod(obj_, jniReadFileSize);
    checkException(env);
    return static_cast<uint64_t>(out);
  }

  uint64_t memoryUsage() const override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    jlong out = env->CallLongMethod(obj_, jniReadFileMemoryUsage);
    checkException(env);
    return static_cast<uint64_t>(out);
  }

  std::string getName() const override {
    return "<JniReadFile>";
  }

  uint64_t getNaturalReadSize() const override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    jlong out = env->CallLongMethod(obj_, jniReadFileGetNaturalReadSize);
    checkException(env);
    return static_cast<uint64_t>(out);
  }

 private:
  jobject obj_;
};

class JniWriteFile : public facebook::velox::WriteFile {
 public:
  explicit JniWriteFile(jobject obj) {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    obj_ = env->NewGlobalRef(obj);
    checkException(env);
  }

  ~JniWriteFile() override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    env->DeleteGlobalRef(obj_);
    checkException(env);
  }

  void append(std::string_view data) override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    env->CallVoidMethod(obj_, jniWriteFileAppend);
    checkException(env);
  }

  void flush() override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    env->CallVoidMethod(obj_, jniWriteFileFlush);
    checkException(env);
  }

  void close() override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    env->CallVoidMethod(obj_, jniWriteFileClose);
    checkException(env);
  }

  uint64_t size() const override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    jlong out = env->CallLongMethod(obj_, jniWriteFileSize);
    checkException(env);
    return static_cast<uint64_t>(out);
  }

 private:
  jobject obj_;
};

// Convert "xxx:/a/b/c" to "/a/b/c". Probably it's Velox's job to remove the protocol when calling the member
// functions?
class RemovePathProtocol : public facebook::velox::filesystems::FileSystem {
 public:
  static std::shared_ptr<facebook::velox::filesystems::FileSystem> wrap(
      std::shared_ptr<facebook::velox::filesystems::FileSystem> fs) {
    return std::shared_ptr<facebook::velox::filesystems::FileSystem>(new RemovePathProtocol(fs));
  }

  std::string name() const override {
    return fs_->name();
  }

  std::unique_ptr<facebook::velox::ReadFile> openFileForRead(
      std::string_view path,
      const facebook::velox::filesystems::FileOptions& options) override {
    return fs_->openFileForRead(rewrite(path), options);
  }

  std::unique_ptr<facebook::velox::WriteFile> openFileForWrite(
      std::string_view path,
      const facebook::velox::filesystems::FileOptions& options) override {
    return fs_->openFileForWrite(rewrite(path), options);
  }

  void remove(std::string_view path) override {
    fs_->remove(rewrite(path));
  }

  void rename(std::string_view oldPath, std::string_view newPath, bool overwrite) override {
    fs_->rename(rewrite(oldPath), rewrite(newPath), overwrite);
  }

  bool exists(std::string_view path) override {
    return fs_->exists(rewrite(path));
  }

  std::vector<std::string> list(std::string_view path) override {
    return fs_->list(rewrite(path));
  }

  void mkdir(std::string_view path) override {
    fs_->mkdir(rewrite(path));
  }

  void rmdir(std::string_view path) override {
    fs_->rmdir(rewrite(path));
  }

 private:
  RemovePathProtocol(std::shared_ptr<facebook::velox::filesystems::FileSystem> fs) : FileSystem({}), fs_(fs) {}

  std::string_view rewrite(std::string_view path) {
    return removePathProtocol(path);
  }

  std::shared_ptr<facebook::velox::filesystems::FileSystem> fs_;
};

class JniFileSystem : public facebook::velox::filesystems::FileSystem {
 public:
  explicit JniFileSystem(jobject obj, std::shared_ptr<const facebook::velox::Config> config) : FileSystem(config) {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    obj_ = env->NewGlobalRef(obj);
    checkException(env);
  }

  ~JniFileSystem() override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    env->DeleteGlobalRef(obj_);
    checkException(env);
  }

  std::string name() const override {
    return "JNI FS";
  }

  std::unique_ptr<facebook::velox::ReadFile> openFileForRead(
      std::string_view path,
      const facebook::velox::filesystems::FileOptions& options) override {
    GLUTEN_CHECK(
        !options.values.empty(),
        "JniFileSystem::openFileForRead: file options is not empty, this is not currently supported");
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    jobject obj = env->CallObjectMethod(obj_, jniFileSystemOpenFileForRead, createJString(env, path));
    auto out = std::make_unique<JniReadFile>(obj);
    checkException(env);
    return out;
  }

  std::unique_ptr<facebook::velox::WriteFile> openFileForWrite(
      std::string_view path,
      const facebook::velox::filesystems::FileOptions& options) override {
    GLUTEN_CHECK(
        !options.values.empty(),
        "JniFileSystem::openFileForWrite: file options is not empty, this is not currently supported");
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    jobject obj = env->CallObjectMethod(obj_, jniFileSystemOpenFileForWrite, createJString(env, path));
    auto out = std::make_unique<JniWriteFile>(obj);
    checkException(env);
    return out;
  }

  void remove(std::string_view path) override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    env->CallVoidMethod(obj_, jniFileSystemRemove, createJString(env, path));
    checkException(env);
  }

  void rename(std::string_view oldPath, std::string_view newPath, bool overwrite) override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    env->CallVoidMethod(obj_, jniFileSystemRename, createJString(env, oldPath), createJString(env, newPath), overwrite);
    checkException(env);
  }

  bool exists(std::string_view path) override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    bool out = env->CallBooleanMethod(obj_, jniFileSystemExists, createJString(env, path));
    checkException(env);
    return out;
  }

  std::vector<std::string> list(std::string_view path) override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    std::vector<std::string> out;
    jobjectArray jarray = (jobjectArray)env->CallObjectMethod(obj_, jniFileSystemList, createJString(env, path));
    jsize length = env->GetArrayLength(jarray);
    for (jsize i = 0; i < length; ++i) {
      jstring element = (jstring)env->GetObjectArrayElement(jarray, i);
      std::string cElement = jStringToCString(env, element);
      out.push_back(cElement);
    }
    checkException(env);
    return out;
  }

  void mkdir(std::string_view path) override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    env->CallVoidMethod(obj_, jniFileSystemMkdir, createJString(env, path));
    checkException(env);
  }

  void rmdir(std::string_view path) override {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    env->CallVoidMethod(obj_, jniFileSystemRmdir, createJString(env, path));
    checkException(env);
  }

  static bool isCapableForNewFile(uint64_t size) {
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(vm, &env);
    bool out = env->CallStaticBooleanMethod(jniFileSystemClass, jniIsCapableForNewFile, static_cast<jlong>(size));
    checkException(env);
    return out;
  }

  static std::function<bool(std::string_view)> schemeMatcher() {
    return [](std::string_view filePath) { return filePath.find(kJniFsScheme) == 0; };
  }

  static std::function<std::shared_ptr<FileSystem>(std::shared_ptr<const facebook::velox::Config>, std::string_view)>
  fileSystemGenerator() {
    return [](std::shared_ptr<const facebook::velox::Config> properties, std::string_view filePath) {
      JNIEnv* env;
      attachCurrentThreadAsDaemonOrThrow(vm, &env);
      jobject obj = env->CallStaticObjectMethod(jniFileSystemClass, jniGetFileSystem);
      // remove "jni:" or "jol:" prefix.
      std::shared_ptr<FileSystem> lfs = RemovePathProtocol::wrap(std::make_shared<JniFileSystem>(obj, properties));
      checkException(env);
      return lfs;
    };
  }

 private:
  jobject obj_;
};

// "jol" stands for letting Gluten choose between jni fs and local fs.
// This doesn't implement facebook::velox::filesystems::FileSystem since it just
// act as a entry-side router to create JniFilesystem and LocalFilesystem
class JolFileSystem {
  static class std::shared_ptr<JolFileSystem> create(uint64_t maxFileSize) {
    return std::shared_ptr<JolFileSystem>(new JolFileSystem(maxFileSize));
  }

  std::function<bool(std::string_view)>
  schemeMatcher() {
    return [](std::string_view filePath) { return filePath.find(kJolFsScheme) == 0; };
  }

  std::function<std::shared_ptr<
      facebook::velox::filesystems::FileSystem>(std::shared_ptr<const facebook::velox::Config>, std::string_view)>
  fileSystemGenerator() {
    return [=](std::shared_ptr<const facebook::velox::Config> properties,
               std::string_view filePath) -> std::shared_ptr<facebook::velox::filesystems::FileSystem> {
      if (JniFileSystem::isCapableForNewFile(maxFileSize_)) {
        return JniFileSystem::fileSystemGenerator()(properties, filePath);
      }
      const std::string_view& localFilePath =
          removePathProtocol(filePath); // remove "jol:" to make Velox choose local fs.
      auto fs = RemovePathProtocol::wrap(facebook::velox::filesystems::getFileSystem(
          localFilePath, properties)); // remove all the "jol:"s in calls to local fs
      return fs;
    };
  }

 private:
  JolFileSystem(uint64_t maxFileSize) : maxFileSize_(maxFileSize){};
  uint64_t maxFileSize_;
};
} // namespace

void gluten::initVeloxJniFileSystem(JNIEnv* env) {
  // vm
  if (env->GetJavaVM(&vm) != JNI_OK) {
    throw gluten::GlutenException("Unable to get JavaVM instance");
  }

  // classes
  jniFileSystemClass = createGlobalClassReferenceOrError(env, "Lio/glutenproject/fs/JniFilesystem;");
  jniReadFileClass = createGlobalClassReferenceOrError(env, "Lio/glutenproject/fs/JniFilesystem$ReadFile;");
  jniWriteFileClass = createGlobalClassReferenceOrError(env, "Lio/glutenproject/fs/JniFilesystem$WriteFile;");

  // methods in JniFilesystem
  jniGetFileSystem =
      getStaticMethodIdOrError(env, jniFileSystemClass, "getFileSystem", "()Lio/glutenproject/fs/JniFilesystem;");
  jniIsCapableForNewFile = getStaticMethodIdOrError(env, jniFileSystemClass, "isCapableForNewFile", "(J)Z");
  jniFileSystemOpenFileForRead = getMethodIdOrError(
      env, jniFileSystemClass, "openFileForRead", "(Ljava/lang/String;)Lio/glutenproject/fs/JniFilesystem$ReadFile;");
  jniFileSystemOpenFileForWrite = getMethodIdOrError(
      env, jniFileSystemClass, "openFileForWrite", "(Ljava/lang/String;)Lio/glutenproject/fs/JniFilesystem$WriteFile;");
  jniFileSystemRemove = getMethodIdOrError(env, jniFileSystemClass, "remove", "(Ljava/lang/String;)V");
  jniFileSystemRename =
      getMethodIdOrError(env, jniFileSystemClass, "rename", "(Ljava/lang/String;Ljava/lang/String;Z)V");
  jniFileSystemExists = getMethodIdOrError(env, jniFileSystemClass, "exists", "(Ljava/lang/String;)Z");
  jniFileSystemList = getMethodIdOrError(env, jniFileSystemClass, "list", "(Ljava/lang/String;)[Ljava/lang/String;");
  jniFileSystemMkdir = getMethodIdOrError(env, jniFileSystemClass, "mkdir", "(Ljava/lang/String;)V");
  jniFileSystemRmdir = getMethodIdOrError(env, jniFileSystemClass, "rmdir", "(Ljava/lang/String;)V");

  // methods in JniFilesystem$ReadFile
  jniReadFilePread = getMethodIdOrError(env, jniReadFileClass, "pread", "(JJJ)V");
  jniReadFileShouldCoalesce = getMethodIdOrError(env, jniReadFileClass, "shouldCoalesce", "()Z");
  jniReadFileSize = getMethodIdOrError(env, jniReadFileClass, "size", "()J");
  jniReadFileMemoryUsage = getMethodIdOrError(env, jniReadFileClass, "memoryUsage", "()J");
  jniReadFileGetNaturalReadSize = getMethodIdOrError(env, jniReadFileClass, "getNaturalReadSize", "()J");

  // methods in JniFilesystem$WriteFile
  jniWriteFileAppend = getMethodIdOrError(env, jniWriteFileClass, "append", "([B)V");
  jniWriteFileFlush = getMethodIdOrError(env, jniWriteFileClass, "flush", "()V");
  jniWriteFileClose = getMethodIdOrError(env, jniWriteFileClass, "close", "()V");
  jniWriteFileSize = getMethodIdOrError(env, jniWriteFileClass, "size", "()J");
}

void gluten::finalizeVeloxJniFileSystem(JNIEnv* env) {
  env->DeleteGlobalRef(jniWriteFileClass);
  env->DeleteGlobalRef(jniReadFileClass);
  env->DeleteGlobalRef(jniFileSystemClass);

  vm = nullptr;
}

void gluten::registerJniFileSystem() {
  facebook::velox::filesystems::registerFileSystem(
      JniFileSystem::schemeMatcher(), JniFileSystem::fileSystemGenerator());
}
