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
package org.apache.gluten.qt.writer

import java.io.File
import java.nio.file.Path

object WriteUtils {
  def deleteFileIfPresent(filePath: String): Boolean = {
    val file = new File(filePath)
    if (file.exists()) {
      file.delete()
    } else {
      false
    }
  }

  def getUniquePath(filePath: Path): Path = {
    if (new File(filePath.toUri.getPath).exists()) {
      var counter = 1
      val splitPath = filePath.getFileName.toString.split("[.]")
      val namePart = splitPath.init.mkString(".")
      val extensionPart = splitPath.last
      var newFilePath = filePath.getParent.resolve(s"${namePart}_$counter.$extensionPart")
      while (new File(newFilePath.toUri.getPath).exists()) {
        counter = counter + 1
        newFilePath = filePath.getParent.resolve(s"${namePart}_$counter.$extensionPart")
      }
      newFilePath
    } else {
      filePath
    }
  }
}
