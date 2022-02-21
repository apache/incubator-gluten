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

#include "protobuf_utils.h"

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

DataTypePtr ProtoTypeToTime32(const exprs::ExtGandivaType& ext_type) {
  switch (ext_type.timeunit()) {
    case exprs::SEC:
      return arrow::time32(arrow::TimeUnit::SECOND);
    case exprs::MILLISEC:
      return arrow::time32(arrow::TimeUnit::MILLI);
    default:
      std::cerr << "Unknown time unit: " << ext_type.timeunit() << " for time32\n";
      return nullptr;
  }
}

DataTypePtr ProtoTypeToTime64(const exprs::ExtGandivaType& ext_type) {
  switch (ext_type.timeunit()) {
    case exprs::MICROSEC:
      return arrow::time64(arrow::TimeUnit::MICRO);
    case exprs::NANOSEC:
      return arrow::time64(arrow::TimeUnit::NANO);
    default:
      std::cerr << "Unknown time unit: " << ext_type.timeunit() << " for time64\n";
      return nullptr;
  }
}

DataTypePtr ProtoTypeToTimestamp(const exprs::ExtGandivaType& ext_type) {
  arrow::TimeUnit::type unit;
  switch (ext_type.timeunit()) {
    case exprs::SEC:
      unit = arrow::TimeUnit::SECOND;
      break;
    case exprs::MILLISEC:
      unit = arrow::TimeUnit::MILLI;
      break;
    case exprs::MICROSEC:
      unit = arrow::TimeUnit::MICRO;
      break;
    case exprs::NANOSEC:
      unit = arrow::TimeUnit::NANO;
      break;
    default:
      std::cerr << "Unknown time unit: " << ext_type.timeunit() << " for timestamp\n";
      return nullptr;
  }
  const std::string& zone_id = ext_type.timezone();
  return arrow::timestamp(unit, zone_id);
}

DataTypePtr ProtoTypeToDataType(const exprs::ExtGandivaType& ext_type) {
  switch (ext_type.type()) {
    case exprs::NONE:
      return arrow::null();
    case exprs::BOOL:
      return arrow::boolean();
    case exprs::UINT8:
      return arrow::uint8();
    case exprs::INT8:
      return arrow::int8();
    case exprs::UINT16:
      return arrow::uint16();
    case exprs::INT16:
      return arrow::int16();
    case exprs::UINT32:
      return arrow::uint32();
    case exprs::INT32:
      return arrow::int32();
    case exprs::UINT64:
      return arrow::uint64();
    case exprs::INT64:
      return arrow::int64();
    case exprs::HALF_FLOAT:
      return arrow::float16();
    case exprs::FLOAT:
      return arrow::float32();
    case exprs::DOUBLE:
      return arrow::float64();
    case exprs::UTF8:
      return arrow::utf8();
    case exprs::BINARY:
      return arrow::binary();
    case exprs::DATE32:
      return arrow::date32();
    case exprs::DATE64:
      return arrow::date64();
    case exprs::DECIMAL:
      // TODO: error handling
      return arrow::decimal(ext_type.precision(), ext_type.scale());
    case exprs::TIME32:
      return ProtoTypeToTime32(ext_type);
    case exprs::TIME64:
      return ProtoTypeToTime64(ext_type);
    case exprs::TIMESTAMP:
      return ProtoTypeToTimestamp(ext_type);

    case exprs::FIXED_SIZE_BINARY:
    case exprs::INTERVAL:
    case exprs::LIST:
    case exprs::STRUCT:
    case exprs::UNION:
    case exprs::DICTIONARY:
    case exprs::MAP:
      std::cerr << "Unhandled data type: " << ext_type.type() << "\n";
      return nullptr;

    default:
      std::cerr << "Unknown data type: " << ext_type.type() << "\n";
      return nullptr;
  }
}

FieldPtr ProtoTypeToField(const exprs::Field& f) {
  const std::string& name = f.name();
  DataTypePtr type = ProtoTypeToDataType(f.type());
  bool nullable = true;
  if (f.has_nullable()) {
    nullable = f.nullable();
  }

  return field(name, type, nullable);
}

NodePtr ProtoTypeToFieldNode(const exprs::FieldNode& node) {
  FieldPtr field_ptr = ProtoTypeToField(node.field());
  if (field_ptr == nullptr) {
    std::cerr << "Unable to create field node from protobuf\n";
    return nullptr;
  }

  return TreeExprBuilder::MakeField(field_ptr);
}

NodePtr ProtoTypeToFnNode(const exprs::FunctionNode& node) {
  const std::string& name = node.functionname();
  NodeVector children;

  for (int i = 0; i < node.inargs_size(); i++) {
    const exprs::TreeNode& arg = node.inargs(i);

    NodePtr n = ProtoTypeToNode(arg);
    if (n == nullptr) {
      std::cerr << "Unable to create argument for function: " << name << "\n";
      return nullptr;
    }

    children.push_back(n);
  }

  DataTypePtr return_type = ProtoTypeToDataType(node.returntype());
  if (return_type == nullptr) {
    std::cerr << "Unknown return type for function: " << name << "\n";
    return nullptr;
  }

  return TreeExprBuilder::MakeFunction(name, children, return_type);
}

NodePtr ProtoTypeToIfNode(const exprs::IfNode& node) {
  NodePtr cond = ProtoTypeToNode(node.cond());
  if (cond == nullptr) {
    std::cerr << "Unable to create cond node for if node\n";
    return nullptr;
  }

  NodePtr then_node = ProtoTypeToNode(node.thennode());
  if (then_node == nullptr) {
    std::cerr << "Unable to create then node for if node\n";
    return nullptr;
  }

  NodePtr else_node = ProtoTypeToNode(node.elsenode());
  if (else_node == nullptr) {
    std::cerr << "Unable to create else node for if node\n";
    return nullptr;
  }

  DataTypePtr return_type = ProtoTypeToDataType(node.returntype());
  if (return_type == nullptr) {
    std::cerr << "Unknown return type for if node\n";
    return nullptr;
  }

  return TreeExprBuilder::MakeIf(cond, then_node, else_node, return_type);
}

NodePtr ProtoTypeToAndNode(const exprs::AndNode& node) {
  NodeVector children;

  for (int i = 0; i < node.args_size(); i++) {
    const exprs::TreeNode& arg = node.args(i);

    NodePtr n = ProtoTypeToNode(arg);
    if (n == nullptr) {
      std::cerr << "Unable to create argument for boolean and\n";
      return nullptr;
    }
    children.push_back(n);
  }
  return TreeExprBuilder::MakeAnd(children);
}

NodePtr ProtoTypeToOrNode(const exprs::OrNode& node) {
  NodeVector children;

  for (int i = 0; i < node.args_size(); i++) {
    const exprs::TreeNode& arg = node.args(i);

    NodePtr n = ProtoTypeToNode(arg);
    if (n == nullptr) {
      std::cerr << "Unable to create argument for boolean or\n";
      return nullptr;
    }
    children.push_back(n);
  }
  return TreeExprBuilder::MakeOr(children);
}

NodePtr ProtoTypeToInNode(const exprs::InNode& node) {
  NodePtr field = ProtoTypeToNode(node.node());

  if (node.has_intvalues()) {
    std::unordered_set<int32_t> int_values;
    for (int i = 0; i < node.intvalues().intvalues_size(); i++) {
      int_values.insert(node.intvalues().intvalues(i).value());
    }
    return TreeExprBuilder::MakeInExpressionInt32(field, int_values);
  }

  if (node.has_longvalues()) {
    std::unordered_set<int64_t> long_values;
    for (int i = 0; i < node.longvalues().longvalues_size(); i++) {
      long_values.insert(node.longvalues().longvalues(i).value());
    }
    return TreeExprBuilder::MakeInExpressionInt64(field, long_values);
  }

  if (node.has_stringvalues()) {
    std::unordered_set<std::string> stringvalues;
    for (int i = 0; i < node.stringvalues().stringvalues_size(); i++) {
      stringvalues.insert(node.stringvalues().stringvalues(i).value());
    }
    return TreeExprBuilder::MakeInExpressionString(field, stringvalues);
  }

  if (node.has_binaryvalues()) {
    std::unordered_set<std::string> stringvalues;
    for (int i = 0; i < node.binaryvalues().binaryvalues_size(); i++) {
      stringvalues.insert(node.binaryvalues().binaryvalues(i).value());
    }
    return TreeExprBuilder::MakeInExpressionBinary(field, stringvalues);
  }
  // not supported yet.
  std::cerr << "Unknown constant type for in expression.\n";
  return nullptr;
}

NodePtr ProtoTypeToNullNode(const exprs::NullNode& node) {
  DataTypePtr data_type = ProtoTypeToDataType(node.type());
  if (data_type == nullptr) {
    std::cerr << "Unknown type " << data_type->ToString() << " for null node\n";
    return nullptr;
  }

  return TreeExprBuilder::MakeNull(data_type);
}

NodePtr ProtoTypeToNode(const exprs::TreeNode& node) {
  if (node.has_fieldnode()) {
    return ProtoTypeToFieldNode(node.fieldnode());
  }

  if (node.has_fnnode()) {
    return ProtoTypeToFnNode(node.fnnode());
  }

  if (node.has_ifnode()) {
    return ProtoTypeToIfNode(node.ifnode());
  }

  if (node.has_andnode()) {
    return ProtoTypeToAndNode(node.andnode());
  }

  if (node.has_ornode()) {
    return ProtoTypeToOrNode(node.ornode());
  }

  if (node.has_innode()) {
    return ProtoTypeToInNode(node.innode());
  }

  if (node.has_nullnode()) {
    return ProtoTypeToNullNode(node.nullnode());
  }

  if (node.has_intnode()) {
    return TreeExprBuilder::MakeLiteral(node.intnode().value());
  }

  if (node.has_floatnode()) {
    return TreeExprBuilder::MakeLiteral(node.floatnode().value());
  }

  if (node.has_longnode()) {
    return TreeExprBuilder::MakeLiteral(node.longnode().value());
  }

  if (node.has_booleannode()) {
    return TreeExprBuilder::MakeLiteral(node.booleannode().value());
  }

  if (node.has_doublenode()) {
    return TreeExprBuilder::MakeLiteral(node.doublenode().value());
  }

  if (node.has_stringnode()) {
    return TreeExprBuilder::MakeStringLiteral(node.stringnode().value());
  }

  if (node.has_binarynode()) {
    return TreeExprBuilder::MakeBinaryLiteral(node.binarynode().value());
  }

  if (node.has_decimalnode()) {
    std::string value = node.decimalnode().value();
    gandiva::DecimalScalar128 literal(value, node.decimalnode().precision(),
                                      node.decimalnode().scale());
    return TreeExprBuilder::MakeDecimalLiteral(literal);
  }
  std::cerr << "Unknown node type in protobuf\n";
  return nullptr;
}

ExpressionPtr ProtoTypeToExpression(const exprs::ExpressionRoot& root) {
  NodePtr root_node = ProtoTypeToNode(root.root());
  if (root_node == nullptr) {
    std::cerr << "Unable to create expression node from expression protobuf\n";
    return nullptr;
  }

  FieldPtr field = ProtoTypeToField(root.resulttype());
  if (field == nullptr) {
    std::cerr << "Unable to extra return field from expression protobuf\n";
    return nullptr;
  }

  return TreeExprBuilder::MakeExpression(root_node, field);
}

ConditionPtr ProtoTypeToCondition(const exprs::Condition& condition) {
  NodePtr root_node = ProtoTypeToNode(condition.root());
  if (root_node == nullptr) {
    return nullptr;
  }

  return TreeExprBuilder::MakeCondition(root_node);
}

SchemaPtr ProtoTypeToSchema(const exprs::Schema& schema) {
  std::vector<FieldPtr> fields;

  for (int i = 0; i < schema.columns_size(); i++) {
    FieldPtr field = ProtoTypeToField(schema.columns(i));
    if (field == nullptr) {
      std::cerr << "Unable to extract arrow field from schema\n";
      return nullptr;
    }

    fields.push_back(field);
  }

  return arrow::schema(fields);
}

// Common for both projector and filters.

bool ParseProtobuf(uint8_t* buf, int bufLen, google::protobuf::Message* msg) {
  google::protobuf::io::CodedInputStream cis(buf, bufLen);
  cis.SetRecursionLimit(1000);
  return msg->ParseFromCodedStream(&cis);
}
