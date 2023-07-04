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

namespace gluten {

thread_local std::unique_ptr<TaskContextStorage> taskContextStorage = nullptr;

void bindToTask(std::shared_ptr<void> object) {
  GLUTEN_CHECK(taskContextStorage != nullptr, "Not in a Spark task");
  taskContextStorage->bind(object);
}

void bindToTaskIfPossible(std::shared_ptr<void> object) {
  if (taskContextStorage == nullptr) {
    return; // it's likely that we are not in a Spark task
  }
  taskContextStorage->bind(object);
}

void createTaskContextStorage() {
  GLUTEN_CHECK(taskContextStorage == nullptr, "Task context storage is already created");
  taskContextStorage = std::make_unique<TaskContextStorage>();
}

void deleteTaskContextStorage() {
  GLUTEN_CHECK(taskContextStorage != nullptr, "Task context storage is not created");
  taskContextStorage = nullptr;
}
} // namespace gluten
