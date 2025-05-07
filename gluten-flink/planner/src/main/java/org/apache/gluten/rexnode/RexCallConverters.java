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

package org.apache.gluten.rexnode;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.calcite.rex.RexCall;
import org.apache.gluten.rexnode.RexNodeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

abstract class BaseRexCallConverter implements RexCallConverter {
    private static final Logger LOG = LoggerFactory.getLogger(BaseRexCallConverter.class);
    protected final String rexOperatorName;

    public BaseRexCallConverter(String rexOperatorName) {
        this.rexOperatorName = rexOperatorName;
    }

    protected List<TypedExpr> getParams(RexCall callNode, List<String> inNames) {
        LOG.debug("Converting function: {}", rexOperatorName);
        return RexNodeConverter.toTypedExpr(callNode.getOperands(), inNames);
    }

    protected Type getResultType(RexCall callNode) {
        return RexNodeConverter.toType(callNode.getType());
    }
}

class DefaultRexCallConverter extends BaseRexCallConverter {
    public DefaultRexCallConverter(String rexOperatorName) {
        super(rexOperatorName);
    }

    @Override
    public TypedExpr toTypedExpr(RexCall callNode, List<String> inNames) {
        List<TypedExpr> params = getParams(callNode, inNames);
        Type resultType = getResultType(callNode);
        return new CallTypedExpr(resultType, params, rexOperatorName);
    }
}

class AlignInputsTypeRexCallConverter extends BaseRexCallConverter {
    public AlignInputsTypeRexCallConverter(String rexOperatorName) {
        super(rexOperatorName);
    }

    @Override
    public TypedExpr toTypedExpr(RexCall callNode, List<String> inNames) {
        List<TypedExpr> params = getParams(callNode, inNames);
        List<TypedExpr> alignedParams = Utils.promoteTypeForNumberExpressions(params);
        Type resultType = getResultType(callNode);
        return new CallTypedExpr(resultType, alignedParams, rexOperatorName);
    }
}
