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
#include <Parser/SerializedPlanParser.h>
#include <google/protobuf/util/json_util.h>
#include <gtest/gtest.h>

using namespace local_engine;
using namespace DB;

std::string splitBinaryFromJson(const std::string & json)
{
    std::string binary;
    substrait::ReadRel::LocalFiles local_files;
    auto s = google::protobuf::util::JsonStringToMessage(absl::string_view(json), &local_files);
    local_files.SerializeToString(&binary);
    return binary;
}

std::string JsonPlanFor65234()
{
    // Plan for https://github.com/ClickHouse/ClickHouse/pull/65234
    return R"(
{
  "extensions": [{
    "extensionFunction": {
      "functionAnchor": 1,
      "name": "is_not_null:str"
    }
  }, {
    "extensionFunction": {
      "functionAnchor": 2,
      "name": "equal:str_str"
    }
  }, {
    "extensionFunction": {
      "functionAnchor": 3,
      "name": "is_not_null:i64"
    }
  }, {
    "extensionFunction": {
      "name": "and:bool_bool"
    }
  }],
  "relations": [{
    "root": {
      "input": {
        "project": {
          "common": {
            "emit": {
              "outputMapping": [2]
            }
          },
          "input": {
            "filter": {
              "common": {
                "direct": {
                }
              },
              "input": {
                "read": {
                  "common": {
                    "direct": {
                    }
                  },
                  "baseSchema": {
                    "names": ["r_regionkey", "r_name"],
                    "struct": {
                      "types": [{
                        "i64": {
                          "nullability": "NULLABILITY_NULLABLE"
                        }
                      }, {
                        "string": {
                          "nullability": "NULLABILITY_NULLABLE"
                        }
                      }]
                    },
                    "columnTypes": ["NORMAL_COL", "NORMAL_COL"]
                  },
                  "filter": {
                    "scalarFunction": {
                      "outputType": {
                        "bool": {
                          "nullability": "NULLABILITY_NULLABLE"
                        }
                      },
                      "arguments": [{
                        "value": {
                          "scalarFunction": {
                            "outputType": {
                              "bool": {
                                "nullability": "NULLABILITY_NULLABLE"
                              }
                            },
                            "arguments": [{
                              "value": {
                                "scalarFunction": {
                                  "functionReference": 1,
                                  "outputType": {
                                    "bool": {
                                      "nullability": "NULLABILITY_REQUIRED"
                                    }
                                  },
                                  "arguments": [{
                                    "value": {
                                      "selection": {
                                        "directReference": {
                                          "structField": {
                                            "field": 1
                                          }
                                        }
                                      }
                                    }
                                  }]
                                }
                              }
                            }, {
                              "value": {
                                "scalarFunction": {
                                  "functionReference": 2,
                                  "outputType": {
                                    "bool": {
                                      "nullability": "NULLABILITY_NULLABLE"
                                    }
                                  },
                                  "arguments": [{
                                    "value": {
                                      "selection": {
                                        "directReference": {
                                          "structField": {
                                            "field": 1
                                          }
                                        }
                                      }
                                    }
                                  }, {
                                    "value": {
                                      "literal": {
                                        "string": "EUROPE"
                                      }
                                    }
                                  }]
                                }
                              }
                            }]
                          }
                        }
                      }, {
                        "value": {
                          "scalarFunction": {
                            "functionReference": 3,
                            "outputType": {
                              "bool": {
                                "nullability": "NULLABILITY_REQUIRED"
                              }
                            },
                            "arguments": [{
                              "value": {
                                "selection": {
                                  "directReference": {
                                    "structField": {
                                    }
                                  }
                                }
                              }
                            }]
                          }
                        }
                      }]
                    }
                  },
                  "advancedExtension": {
                    "optimization": {
                      "@type": "type.googleapis.com/google.protobuf.StringValue",
                      "value": "isMergeTree\u003d0\n"
                    }
                  }
                }
              },
              "condition": {
                "scalarFunction": {
                  "outputType": {
                    "bool": {
                      "nullability": "NULLABILITY_NULLABLE"
                    }
                  },
                  "arguments": [{
                    "value": {
                      "scalarFunction": {
                        "outputType": {
                          "bool": {
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        },
                        "arguments": [{
                          "value": {
                            "scalarFunction": {
                              "functionReference": 1,
                              "outputType": {
                                "bool": {
                                  "nullability": "NULLABILITY_REQUIRED"
                                }
                              },
                              "arguments": [{
                                "value": {
                                  "selection": {
                                    "directReference": {
                                      "structField": {
                                        "field": 1
                                      }
                                    }
                                  }
                                }
                              }]
                            }
                          }
                        }, {
                          "value": {
                            "scalarFunction": {
                              "functionReference": 2,
                              "outputType": {
                                "bool": {
                                  "nullability": "NULLABILITY_NULLABLE"
                                }
                              },
                              "arguments": [{
                                "value": {
                                  "selection": {
                                    "directReference": {
                                      "structField": {
                                        "field": 1
                                      }
                                    }
                                  }
                                }
                              }, {
                                "value": {
                                  "literal": {
                                    "string": "EUROPE"
                                  }
                                }
                              }]
                            }
                          }
                        }]
                      }
                    }
                  }, {
                    "value": {
                      "scalarFunction": {
                        "functionReference": 3,
                        "outputType": {
                          "bool": {
                            "nullability": "NULLABILITY_REQUIRED"
                          }
                        },
                        "arguments": [{
                          "value": {
                            "selection": {
                              "directReference": {
                                "structField": {
                                }
                              }
                            }
                          }
                        }]
                      }
                    }
                  }]
                }
              }
            }
          },
          "expressions": [{
            "selection": {
              "directReference": {
                "structField": {
                }
              }
            }
          }]
        }
      },
      "names": ["r_regionkey#72"],
      "outputSchema": {
        "types": [{
          "i64": {
            "nullability": "NULLABILITY_NULLABLE"
          }
        }],
        "nullability": "NULLABILITY_REQUIRED"
      }
    }
  }]
}
)";
}

TEST(SerializedPlanParser, PR65234)
{
    const std::string split
        = R"({"items":[{"uriFile":"file:///part-00000-16caa751-9774-470c-bd37-5c84c53373c8-c000.snappy.parquet","length":"84633","parquet":{},"schema":{},"metadataColumns":[{}]}]}")";
    SerializedPlanParser parser(SerializedPlanParser::global_context);
    parser.addSplitInfo(splitBinaryFromJson(split));
    parser.parseJson(JsonPlanFor65234());
}
