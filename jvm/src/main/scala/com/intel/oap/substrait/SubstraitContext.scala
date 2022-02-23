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

package com.intel.oap.substrait

import com.intel.oap.substrait.rel.{ExtensionTableNode, LocalFilesNode}

class SubstraitContext() {

  private val functionMap = new java.util.HashMap[String, java.lang.Long]()

  private var localFilesNode: LocalFilesNode = null
  private var extensionTableNode: ExtensionTableNode = null

  def setLocalFilesNode(localFilesNode: LocalFilesNode): Unit = {
    this.localFilesNode = localFilesNode
  }

  def getLocalFilesNode(): LocalFilesNode = this.localFilesNode

  def setExtensionTableNode(extensionTableNode: ExtensionTableNode): Unit = {
    this.extensionTableNode = extensionTableNode
  }

  def getExtensionTableNode(): ExtensionTableNode = this.extensionTableNode

  def registerFunction(funcName: String): java.lang.Long = {
    if (!functionMap.containsKey(funcName)) {
      val newFunctionId: java.lang.Long = functionMap.size.toLong
      functionMap.put(funcName, newFunctionId)
      newFunctionId
    }
    else {
      functionMap.get(funcName)
    }
  }

  def registeredFunction : java.util.HashMap[String, java.lang.Long] = functionMap
}
