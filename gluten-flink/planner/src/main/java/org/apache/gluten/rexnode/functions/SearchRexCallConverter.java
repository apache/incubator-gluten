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
package org.apache.gluten.rexnode.functions;

import org.apache.gluten.rexnode.RexConversionContext;
import org.apache.gluten.rexnode.RexNodeConverter;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.CastTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Sarg;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.type.SqlTypeName.SARG;

public class SearchRexCallConverter extends BaseRexCallConverter {
  public SearchRexCallConverter() {
    super("");
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    if (callNode.getOperands().size() > 1) {
      RexNode param1 = callNode.getOperands().get(1);
      if (param1 instanceof RexLiteral) {
        // for count(*) filter (where price >= 10000 and price < 1000000),
        // flink translate it to SEARCH(bid.price, Sarg[[10000..1000000)]).
        RexLiteral rexLiteral = (RexLiteral) param1;
        if (rexLiteral.getTypeName() == SARG) {
          List<TypedExpr> params = new ArrayList<>();
          TypedExpr col = RexNodeConverter.toTypedExpr(callNode.getOperands().get(0), context);
          params.add(col);
          Sarg sarg = (Sarg) rexLiteral.getValue();
          List<TypedExpr> ranges = RexNodeConverter.toTypedExpr(sarg, rexLiteral.getType());
          if (col.getReturnType().getClass() != ranges.get(0).getReturnType().getClass()) {
            params.add(CastTypedExpr.create(col.getReturnType(), ranges.get(0), true));
            params.add(CastTypedExpr.create(col.getReturnType(), ranges.get(1), true));
          } else {
            params.addAll(ranges);
          }

          Type resultType = getResultType(callNode);
          return new CallTypedExpr(resultType, params, "between");
        }
      }
    }
    return new DefaultRexCallConverter("in").toTypedExpr(callNode, context);
  }
}
