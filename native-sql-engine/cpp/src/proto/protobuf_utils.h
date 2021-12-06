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

#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gandiva/arrow.h>
#include <gandiva/gandiva_aliases.h>
#include <gandiva/tree_expr_builder.h>

#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "Exprs.pb.h"

using gandiva::ConditionPtr;
using gandiva::DataTypePtr;
using gandiva::ExpressionPtr;
using gandiva::ExpressionVector;
using gandiva::FieldPtr;
using gandiva::FieldVector;
using gandiva::NodePtr;
using gandiva::NodeVector;
using gandiva::SchemaPtr;
using gandiva::Status;
using gandiva::TreeExprBuilder;

using gandiva::ArrayDataVector;

// forward declarations
NodePtr ProtoTypeToNode(const exprs::TreeNode& node);

DataTypePtr ProtoTypeToTime32(const exprs::ExtGandivaType& ext_type);
DataTypePtr ProtoTypeToTime64(const exprs::ExtGandivaType& ext_type);
DataTypePtr ProtoTypeToTimestamp(const exprs::ExtGandivaType& ext_type);
DataTypePtr ProtoTypeToDataType(const exprs::ExtGandivaType& ext_type);
FieldPtr ProtoTypeToField(const exprs::Field& f);
NodePtr ProtoTypeToFieldNode(const exprs::FieldNode& node);
NodePtr ProtoTypeToFnNode(const exprs::FunctionNode& node);
NodePtr ProtoTypeToIfNode(const exprs::IfNode& node);
NodePtr ProtoTypeToAndNode(const exprs::AndNode& node);
NodePtr ProtoTypeToOrNode(const exprs::OrNode& node);
NodePtr ProtoTypeToInNode(const exprs::InNode& node);
NodePtr ProtoTypeToNullNode(const exprs::NullNode& node);
NodePtr ProtoTypeToNode(const exprs::TreeNode& node);
ExpressionPtr ProtoTypeToExpression(const exprs::ExpressionRoot& root);
ConditionPtr ProtoTypeToCondition(const exprs::Condition& condition);
SchemaPtr ProtoTypeToSchema(const exprs::Schema& schema);
// Common for both projector and filters.
bool ParseProtobuf(uint8_t* buf, int bufLen, google::protobuf::Message* msg);
