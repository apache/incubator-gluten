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

package io.glutenproject.vectorized;

import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.glutenproject.substrait.plan.PlanNode;
import io.substrait.proto.Plan;

import org.apache.spark.sql.catalyst.InternalRow;

public class ExpressionBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(ExpressionBuilder.class);

  private InputStream in;

  private PlanNode planNode;

  public ExpressionBuilder(InputStream in,
                           PlanNode planNode) {
    this.in = in;
    this.planNode = planNode;
  }

  private native InternalRow[] nativeBuild(InputStream in,
                                           byte[] substraitPlan);

  public InternalRow[] transform() {
    Plan plan = planNode.toProtobuf();
    LOG.debug("ExpressionBuilder exec plan: " + plan.toString());
    return nativeBuild(in, plan.toByteArray());
  }
}
