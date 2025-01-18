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

import org.apache.hadoop.conf.Configuration

/**
 * A concrete implementation of {@link HadoopFileSource} for Google Cloud Storage (GCS). <p> This
 * class customizes the Hadoop {@link Configuration} to support interaction with GCS using the
 * Google Cloud Hadoop connector. It sets the necessary properties for accessing GCS, including the
 * file system implementation and optional service account credentials. <p> Key features: <ul>
 * <li>Configures the Hadoop environment to use {@code GoogleHadoopFileSystem} for GCS paths.</li>
 * <li>Optionally sets up service account authentication using a JSON key file if {@code gcsKeys} is
 * provided in the configuration.</li> </ul>
 */
case class GcsFileSource(conf: QualificationToolConfiguration) extends HadoopFileSource(conf) {
  override protected def hadoopConf: Configuration = {
    val hadoopConf = new Configuration()
    val gcsKeys = conf.gcsKeys
    hadoopConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoopConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    if (gcsKeys != null && gcsKeys.nonEmpty) {
      hadoopConf.set("fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
      hadoopConf.set("fs.gs.auth.service.account.json.keyfile", s"$gcsKeys")
    }
    hadoopConf
  }

}
