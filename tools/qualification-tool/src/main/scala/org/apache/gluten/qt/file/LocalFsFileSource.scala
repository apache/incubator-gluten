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
package org.apache.gluten.qt.file

import org.apache.gluten.qt.QualificationToolConfiguration

/**
 * A concrete implementation of {@link HadoopFileSource} for local file systems. <p> It extends the
 * base functionality of {@code HadoopFileSource} by ensuring that all input paths are correctly
 * prefixed with {@code file://} if they start with a forward slash, indicating a local file system
 * path.
 */

case class LocalFsFileSource(conf: QualificationToolConfiguration) extends HadoopFileSource(conf) {
  override protected def inputPathStrs: Array[String] = {
    super.inputPathStrs.map(
      ip =>
        if (ip.startsWith("/")) {
          s"file://${conf.inputPath}"
        } else {
          ip
        })
  }
}
