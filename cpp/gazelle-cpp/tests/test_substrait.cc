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

#include <arrow/engine/substrait/serde.h>
#include <arrow/result.h>
#include <gtest/gtest.h>
#include <stdio.h>
#include <unistd.h>
#include <fstream>
#include <sstream>
#include "compute/exec_backend.h"
#include "compute/protobuf_utils.h"
#include "compute/substrait_arrow.h"
#include "utils/exception.h"

class TestSubstrait : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
      gazellecpp::compute::Initialize();
      gluten::SetBackendFactory(
      [] { return std::make_shared<gazellecpp::compute::ArrowExecBackend>(); });
    // setenv("MEMKIND_HBW_NODES", "0", 1);
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> getPlanFromFile(
      const std::string& filePath) {
    // Read json file and resume the binary data.
    std::ifstream msgJson(filePath);
    std::stringstream buffer;
    buffer << msgJson.rdbuf();
    std::string msgData = buffer.str();

    auto maybePlan = SubstraitFromJSON("Plan", msgData);
    return maybePlan;
  }
};

TEST_F(TestSubstrait, TestParsePlan) {
  std::string current_path = get_current_dir_name();
  const auto filePath =
      current_path + "/../../gazelle-cpp/tests/data/query.json";
  auto maybePlan = getPlanFromFile(filePath);
  if (!maybePlan.ok()) {
    throw gluten::GlutenException(
        "Can not get plan from file " + filePath +
        " Error: " + maybePlan.status().message());
  };

  auto plan = std::move(maybePlan).ValueOrDie();
  auto backend = gluten::CreateBackend();
  backend->ParsePlan(plan->data(), plan->size());
  arrow::engine::ConvertPlan(backend->GetPlan());
}