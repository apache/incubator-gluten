#include "substrait_to_velox_expr.h"

#include <arrow/array/array_primitive.h>
#include <arrow/array/data.h>
#include <arrow/array/util.h>
#include <arrow/record_batch.h>
#include <arrow/type_fwd.h>

namespace substrait = io::substrait;
using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::connector;
using namespace facebook::velox::dwio::common;

SubstraitVeloxExprConverter::SubstraitVeloxExprConverter(
    const std::shared_ptr<SubstraitParser>& sub_parser,
    const std::unordered_map<uint64_t, std::string>& functions_map) {
  sub_parser_ = sub_parser;
  functions_map_ = functions_map;
}

int32_t SubstraitVeloxExprConverter::parseReferenceSegment(
    const substrait::ReferenceSegment& sref) {
  switch (sref.reference_type_case()) {
    case substrait::ReferenceSegment::ReferenceTypeCase::kStructField: {
      auto sfield = sref.struct_field();
      auto field_id = sfield.field();
      return field_id;
      break;
    }
    default:
      throw new std::runtime_error("not supported");
      break;
  }
}

std::shared_ptr<const core::FieldAccessTypedExpr>
SubstraitVeloxExprConverter::toVeloxExpr(const substrait::FieldReference& sfield,
                                         const int32_t& input_plan_node_id) {
  switch (sfield.reference_type_case()) {
    case substrait::FieldReference::ReferenceTypeCase::kDirectReference: {
      auto dref = sfield.direct_reference();
      int32_t col_idx = parseReferenceSegment(dref);
      auto field_name = sub_parser_->makeNodeName(input_plan_node_id, col_idx);
      // FIXME: How to get the input types?
      return std::make_shared<const core::FieldAccessTypedExpr>(DOUBLE(), field_name);
      break;
    }
    case substrait::FieldReference::ReferenceTypeCase::kMaskedReference: {
      throw new std::runtime_error("not supported");
      break;
    }
    default:
      throw new std::runtime_error("not supported");
      break;
  }
}

std::shared_ptr<const core::ITypedExpr> SubstraitVeloxExprConverter::toVeloxExpr(
    const substrait::Expression::ScalarFunction& sfunc,
    const int32_t& input_plan_node_id) {
  std::vector<std::shared_ptr<const core::ITypedExpr>> params;
  for (auto& sarg : sfunc.args()) {
    auto expr = toVeloxExpr(sarg, input_plan_node_id);
    params.push_back(expr);
  }
  auto function_id = sfunc.id().id();
  auto function_name = sub_parser_->findFunction(functions_map_, function_id);
  auto out_type = sfunc.output_type();
  auto sub_type = sub_parser_->parseType(out_type);
  auto velox_type = sub_parser_->getVeloxType(sub_type->name);
  return std::make_shared<const core::CallTypedExpr>(velox_type, std::move(params),
                                                     function_name);
}

std::shared_ptr<const core::ConstantTypedExpr> SubstraitVeloxExprConverter::toVeloxExpr(
    const io::substrait::Expression::Literal& slit) {
  switch (slit.literal_type_case()) {
    case substrait::Expression_Literal::LiteralTypeCase::kFp64: {
      double val = slit.fp64();
      return std::make_shared<core::ConstantTypedExpr>(val);
      break;
    }
    case substrait::Expression_Literal::LiteralTypeCase::kBoolean: {
      bool val = slit.boolean();
      throw new std::runtime_error("Type is not supported.");
      break;
    }
    default:
      throw new std::runtime_error("Type is not supported.");
      break;
  }
}

std::shared_ptr<const core::ITypedExpr> SubstraitVeloxExprConverter::toVeloxExpr(
    const substrait::Expression& sexpr, const int32_t& input_plan_node_id) {
  std::shared_ptr<const core::ITypedExpr> velox_expr;
  switch (sexpr.rex_type_case()) {
    case substrait::Expression::RexTypeCase::kLiteral: {
      auto slit = sexpr.literal();
      velox_expr = toVeloxExpr(slit);
      break;
    }
    case substrait::Expression::RexTypeCase::kScalarFunction: {
      auto sfunc = sexpr.scalar_function();
      velox_expr = toVeloxExpr(sfunc, input_plan_node_id);
      break;
    }
    case substrait::Expression::RexTypeCase::kSelection: {
      auto sel = sexpr.selection();
      velox_expr = toVeloxExpr(sel, input_plan_node_id);
      break;
    }
    default:
      throw new std::runtime_error("Expression not supported");
      break;
  }
  return velox_expr;
}
