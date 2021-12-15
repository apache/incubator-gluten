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

#include <arrow/array.h>
#include <arrow/pretty_print.h>
#include <gtest/gtest.h>

#include <fstream>
#include <iostream>
#include <memory>

#include "proto/substrait_to_velox_plan.h"
#include "proto/substrait_utils.h"

namespace gazellejni {

TEST(TestVeloxCompute, QueryTest) {
  std::fstream sub("./resources/sub.data", std::ios::binary | std::ios::in);
  substrait::Plan ws_plan;
  ws_plan.ParseFromIstream(&sub);
  auto converter = std::make_shared<SubstraitVeloxPlanConverter>();
  auto out_iter = converter->getResIter(converter->toVeloxPlan(ws_plan));
  while (out_iter->HasNext()) {
    std::shared_ptr<arrow::RecordBatch> result_batch;
    out_iter->Next(&result_batch);
    arrow::PrettyPrint(*result_batch.get(), 2, &std::cout);
  }
}

}  // namespace gazellejni
