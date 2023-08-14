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

#include "TaskContext.h"
#include <folly/Likely.h>
#include "utils/exception.h"

#include <iostream>
#include <mutex>

namespace {
class TaskContextStorage {
 public:
  TaskContextStorage(std::string name) : name_(name) {}

  virtual ~TaskContextStorage() {
    for (auto itr = objects_.rbegin(); itr != objects_.rend(); itr++) {
      itr->reset();
    }
  }

  void bind(std::shared_ptr<void> object) {
    objects_.push_back(object);
  }

 private:
  std::vector<std::shared_ptr<void>> objects_;
  const std::string name_;
};

thread_local std::unique_ptr<TaskContextStorage> taskContextStorage = nullptr;
std::unique_ptr<TaskContextStorage> fallbackStorage = std::make_unique<TaskContextStorage>("fallback");
std::mutex fallbackStorageMutex;
} // namespace

namespace gluten {

bool isOnSparkTaskMainThread() {
  return taskContextStorage != nullptr;
}

void bindToTask(std::shared_ptr<void> object) {
  if (isOnSparkTaskMainThread()) {
    taskContextStorage->bind(object);
    return;
  }
  // The fallback storage is used. Spark sometimes creates sub-threads from a task thread. For example,
  //   PythonRunner.scala:183 @ Spark3.2.2
  //   GlutenSubqueryBroadcastExec.scala:73 @6bce2c33
  std::lock_guard<std::mutex> guard(fallbackStorageMutex);
  std::cout << "Binding a shared object to fallback storage. This should only happen on sub-thread of a Spark task. "
            << std::endl;
  fallbackStorage->bind(object);
}

void createTaskContextStorage(std::string name) {
  GLUTEN_CHECK(taskContextStorage == nullptr, "Task context storage is already created");
  taskContextStorage = std::make_unique<TaskContextStorage>(name);
}

void deleteTaskContextStorage() {
  GLUTEN_CHECK(taskContextStorage != nullptr, "Task context storage is not created");
  taskContextStorage.reset();
}
} // namespace gluten
