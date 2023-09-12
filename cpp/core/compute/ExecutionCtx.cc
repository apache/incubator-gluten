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

#include "ExecutionCtx.h"

namespace gluten {

static ExecutionCtxFactoryContext* getExecutionCtxFactoryContext() {
  static ExecutionCtxFactoryContext* executionCtxFactoryCtx = new ExecutionCtxFactoryContext;
  return executionCtxFactoryCtx;
}

void setExecutionCtxFactory(
    ExecutionCtxFactoryWithConf factory,
    const std::unordered_map<std::string, std::string>& sparkConfs) {
  getExecutionCtxFactoryContext()->set(factory, sparkConfs);
#ifdef GLUTEN_PRINT_DEBUG
  std::cout << "Set execution context factory with conf." << std::endl;
#endif
}

void setExecutionCtxFactory(ExecutionCtxFactory factory) {
  getExecutionCtxFactoryContext()->set(factory);
#ifdef GLUTEN_PRINT_DEBUG
  std::cout << "Set execution context factory." << std::endl;
#endif
}

std::shared_ptr<ExecutionCtx> createExecutionCtx() {
  return getExecutionCtxFactoryContext()->create();
}

} // namespace gluten
