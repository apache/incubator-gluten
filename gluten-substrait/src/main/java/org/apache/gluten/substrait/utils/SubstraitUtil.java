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
package org.apache.gluten.substrait.utils;

import org.apache.gluten.backendsapi.BackendsApiManager;

import com.google.protobuf.Any;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;

public class SubstraitUtil {

  private SubstraitUtil() {}

  public static Any convertJavaObjectToAny(Object obj) {
    if (obj == null) return null;
    Message msg = null;
    if (obj instanceof String) {
      msg = StringValue.newBuilder().setValue(obj.toString()).build();
    } else if (obj instanceof Integer) {
      msg = Int32Value.newBuilder().setValue(Integer.valueOf(obj.toString())).build();
    } else if (obj instanceof Long) {
      msg = Int64Value.newBuilder().setValue(Long.valueOf(obj.toString())).build();
    } else if (obj instanceof Double) {
      msg = DoubleValue.newBuilder().setValue(Double.valueOf(obj.toString())).build();
    } else {
      // TODO: generate the message according to the object type
      msg = StringValue.newBuilder().setValue(obj.toString()).build();
    }

    if (msg == null) return null;
    return BackendsApiManager.getTransformerApiInstance().packPBMessage(msg);
  }
}
