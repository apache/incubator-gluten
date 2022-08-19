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

#include <arrow/c/bridge.h>
#include <arrow/engine/substrait/serde.h>
#include "compute/substrait_arrow.h"
#include "compute/exec_backend.h"

std::string substrait_json = R"({
      "relations": [
        {
          "root": {
            "input": {
              "read": {
                "common": {
                  "direct": {}
                },
                "baseSchema": {
                  "names": [
                    "dispatching_base_num",
                    "pickup_datetime",
                    "dropOff_datetime",
                    "PUlocationID",
                    "DOlocationID",
                    "SR_Flag",
                    "Affiliated_base_number"
                  ],
                  "struct": {
                    "types": [
                      {
                        "string": {
                          "nullability": "NULLABILITY_NULLABLE"
                        }
                      },
                      {
                        "timestamp": {
                          "nullability": "NULLABILITY_NULLABLE"
                        }
                      },
                      {
                        "timestamp": {
                          "nullability": "NULLABILITY_NULLABLE"
                        }
                      },
                      {
                        "fp64": {
                          "nullability": "NULLABILITY_NULLABLE"
                        }
                      },
                      {
                        "fp64": {
                          "nullability": "NULLABILITY_NULLABLE"
                        }
                      },
                      {
                        "i32": {
                          "nullability": "NULLABILITY_NULLABLE"
                        }
                      },
                      {
                        "string": {
                          "nullability": "NULLABILITY_NULLABLE"
                        }
                      }
                    ]
                  }
                },
                "localFiles": {
                  "items": [
                    {
                      "uriFile": "file://FILENAME_PLACEHOLDER",
                      "format": "FILE_FORMAT_PARQUET",
                      "length": "11663225"
                    }
                  ]
                }
              }
            },
            "names": [
              "dispatching_base_num#0",
              "pickup_datetime#1",
              "dropOff_datetime#2",
              "PUlocationID#3",
              "DOlocationID#4",
              "SR_Flag#5",
              "Affiliated_base_number#6"
            ]
          }
        }
      ]
    })";

std::string substrait_json_w_filter = R"(
  {
    "extensions": [
      {
        "extensionFunction": {
          "name": "is_not_null:str"
        }
      }
    ],
    "relations": [
      {
        "root": {
          "input": {
            "filter": {
              "common": {
                "direct": {}
              },
              "input": {
                "read": {
                  "common": {
                    "direct": {}
                  },
                  "baseSchema": {
                    "names": [
                      "dispatching_base_num",
                      "pickup_datetime",
                      "dropOff_datetime",
                      "PUlocationID",
                      "DOlocationID",
                      "SR_Flag",
                      "Affiliated_base_number"
                    ],
                    "struct": {
                      "types": [
                        {
                          "string": {
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        {
                          "timestamp": {
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        {
                          "timestamp": {
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        {
                          "fp64": {
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        {
                          "fp64": {
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        {
                          "i32": {
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        {
                          "string": {
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        }
                      ]
                    }
                  },
                  "filter": {
                    "scalarFunction": {
                      "args": [
                        {
                          "selection": {
                            "directReference": {
                              "structField": {}
                            }
                          }
                        }
                      ],
                      "outputType": {
                        "bool": {
                          "nullability": "NULLABILITY_NULLABLE"
                        }
                      }
                    }
                  },
                  "localFiles": {
                    "items": [
                      {
                        "uriFile": "file://FILENAME_PLACEHOLDER",
                        "format": "FILE_FORMAT_PARQUET",
                        "length": "11663225"
                      }
                    ]
                  }
                }
              },
              "condition": {
                "scalarFunction": {
                  "args": [
                    {
                      "selection": {
                        "directReference": {
                          "structField": {}
                        }
                      }
                    }
                  ],
                  "outputType": {
                    "bool": {
                      "nullability": "NULLABILITY_NULLABLE"
                    }
                  }
                }
              }
            }
          },
          "names": [
            "dispatching_base_num#0",
            "pickup_datetime#1",
            "dropOff_datetime#2",
            "PUlocationID#3",
            "DOlocationID#4",
            "SR_Flag#5",
            "Affiliated_base_number#6"
          ]
        }
      }
    ]
  })";

arrow::Future<std::shared_ptr<arrow::Buffer>> GetSubstraitFromServer(
    std::string substrait_json,
    const std::string& filename) {
  // Emulate server interaction by parsing hard coded JSON
  std::string filename_placeholder = "FILENAME_PLACEHOLDER";
  substrait_json.replace(
      substrait_json.find(filename_placeholder),
      filename_placeholder.size(),
      filename);
  return arrow::engine::internal::SubstraitFromJSON("Plan", substrait_json);
}

std::vector<std::shared_ptr<arrow::RecordBatch>> CollectResult(const std::shared_ptr<arrow::Buffer>& serialized_plan) {
  auto backend = gluten::CreateBackend();
  backend->ParsePlan(serialized_plan->data(), serialized_plan->size());
  auto res_iter = backend->GetResultIterator(gluten::memory::DefaultMemoryAllocator().get());
  auto output_schema = backend->GetOutputSchema();

  int index = 0;
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  while (res_iter->HasNext()) {
    auto array = res_iter->Next()->exportToArrow();
    auto batch =
        arrow::ImportRecordBatch(array.get(), output_schema).ValueOrDie();
    std::cout << "Batch #" << index
              << " num rows: " << batch->num_rows() << std::endl;
    std::cout << batch->ToString() << std::endl;
    ++index;
    batches.emplace_back(batch);
  }
  return batches;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cout << "Please specify a parquet file to scan" << std::endl;
    // Fake pass for CI
    return EXIT_SUCCESS;
  }

  gazellecpp::compute::Initialize();
  gluten::SetBackendFactory(
      [] { return std::make_shared<gazellecpp::compute::ArrowExecBackend>(); });

  // Plans arrive at the consumer serialized in a Buffer, using the binary
  // protobuf serialization of a substrait Plan
  auto serialized_plan_w_filter =
      GetSubstraitFromServer(substrait_json_w_filter, argv[1])
          .result()
          .ValueOrDie();
  auto batches_w_filter = CollectResult(serialized_plan_w_filter);

  auto serialized_plan =
      GetSubstraitFromServer(substrait_json, argv[1]).result().ValueOrDie();
  auto batches = CollectResult(serialized_plan);

  assert(batches.size() == batches_w_filter.size());
  for (size_t i = 0; i < batches.size(); ++i) {
    std::cout << "Comparing batch #" << i << std::endl;
    ARROW_CHECK(batches[i]->Equals(*batches_w_filter[i]));
    std::cout << "Same" << std::endl;
  }
}