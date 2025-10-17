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
package org.apache.gluten.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.*;

public class SimpleUDTF extends GenericUDTF {

  static final ObjectInspector LONG_TYPE = PrimitiveObjectInspectorFactory.javaLongObjectInspector;

  private PrimitiveObjectInspector arg0OI = null;

  @Override
  public void close() throws HiveException {}

  @Override
  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
    // Input
    if (argOIs.length != 1) {
      throw new UDFArgumentException(getClass().getSimpleName() + "() takes one arguments");
    }
    arg0OI = (PrimitiveObjectInspector) argOIs[0];

    // Output
    List<String> fieldNames = Collections.singletonList("longResult");
    List<ObjectInspector> fieldOIs = Collections.singletonList(LONG_TYPE);

    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  @Override
  public void process(Object[] args) throws HiveException {
    Object arg0 = arg0OI.getPrimitiveJavaObject(args[0]);
    if (arg0 == null) {
      return;
    }

    Object[] forwardObj = new Long[1];
    forwardObj[0] = ((Long) arg0).longValue();
    forward(forwardObj);
  }
}
