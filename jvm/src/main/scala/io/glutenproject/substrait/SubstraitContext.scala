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

package io.glutenproject.substrait

import io.glutenproject.substrait.ddlplan.InsertOutputNode
import io.glutenproject.substrait.rel.LocalFilesNode

class SubstraitContext extends Serializable {

  private val functionMap = new java.util.HashMap[String, java.lang.Long]()
  private val iteratorNodes = new java.util.HashMap[java.lang.Long, LocalFilesNode]()
  private var localFilesNodesIndex: java.lang.Integer = new java.lang.Integer(0)
  private var localFilesNodes: Seq[java.io.Serializable] = _
  private var iteratorIndex: java.lang.Long = new java.lang.Long(0)
  private var fileFormat: java.util.List[java.lang.Integer] =
    new java.util.ArrayList[java.lang.Integer]()
  private var insertOutputNode: InsertOutputNode = _

  def getFileFormat(): java.util.List[java.lang.Integer] = this.fileFormat

  def setFileFormat(format: java.util.List[java.lang.Integer]): Unit = {
    this.fileFormat = format
  }

  def setIteratorNode(index: java.lang.Long, localFilesNode: LocalFilesNode): Unit = {
    if (iteratorNodes.containsKey(index)) {
      throw new IllegalStateException(s"Iterator index ${index} has been used.")
    }
    iteratorNodes.put(index, localFilesNode)
  }

  def initLocalFilesNodesIndex(localFilesNodesIndex: java.lang.Integer): Unit = {
    this.localFilesNodesIndex = localFilesNodesIndex
  }

  def getLocalFilesNodes: Seq[java.io.Serializable] = this.localFilesNodes

  def getCurrentLocalFileNode: java.io.Serializable = {
    if (getLocalFilesNodes != null && getLocalFilesNodes.size > localFilesNodesIndex) {
      val res = getLocalFilesNodes(localFilesNodesIndex)
      localFilesNodesIndex += 1
      res
    } else {
      throw new IllegalStateException(
        s"LocalFilesNodes index ${localFilesNodesIndex} exceeds the size of the LocalFilesNodes.")
    }
  }

  def setLocalFilesNodes(localFilesNodes: Seq[java.io.Serializable]): Unit = {
    this.localFilesNodes = localFilesNodes
  }

  def getInputIteratorNode(index: java.lang.Long): LocalFilesNode = {
    iteratorNodes.get(index)
  }

  def getInsertOutputNode: InsertOutputNode = this.insertOutputNode

  def setInsertOutputNode(insertOutputNode: InsertOutputNode): Unit = {
    this.insertOutputNode = insertOutputNode
  }

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

  def registeredFunction: java.util.HashMap[String, java.lang.Long] = functionMap

  def nextIteratorIndex: java.lang.Long = {
    val id = this.iteratorIndex
    this.iteratorIndex += 1
    id
  }
}
