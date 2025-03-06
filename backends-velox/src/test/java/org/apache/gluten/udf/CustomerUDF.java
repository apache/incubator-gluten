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
package org.apache.gluten.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * UDF that generates a the link id (MD5 hash) of a URL. Used to join with link join.
 *
 * <p>Usage example:
 *
 * <p>CREATE TEMPORARY FUNCTION linkid AS 'com.pinterest.hadoop.hive.LinkIdUDF';
 */
@Description(
    name = "linkid",
    value = "_FUNC_(String) - Returns linkid as String, it's the MD5 hash of url.")
public class CustomerUDF extends UDF {
  public String evaluate(String url) {
    if (url == null || url == "") {
      return "";
    }
    return "extendedudf" + url;
  }
}
