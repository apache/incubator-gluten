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
package org.apache.gluten.utils

import org.apache.gluten.backendsapi.clickhouse.GlutenObjectStorageConfig

import org.apache.spark.SparkConf

import io.minio.{BucketExistsArgs, ListObjectsArgs, MakeBucketArgs, MinioClient, RemoveBucketArgs, RemoveObjectsArgs}
import io.minio.messages.DeleteObject
import org.apache.commons.io.FileUtils

import java.io.File
import java.util

import scala.collection.mutable

class MinioTestHelper(TMP_PREFIX: String) {

  // MINIO parameters
  val MINIO_ENDPOINT: String = "http://127.0.0.1:9000/"
  val S3_ACCESS_KEY = "minioadmin"
  val S3_SECRET_KEY = "minioadmin"

  // Object Store parameters
  val S3_METADATA_PATH = s"$TMP_PREFIX/s3/metadata"
  val S3_CACHE_PATH = s"$TMP_PREFIX/s3/cache"
  val S3A_ENDPOINT = "s3a://"

  val STORE_POLICY = "__s3_main"
  val STORE_POLICY_NOCACHE = "__s3_main_2"

  private lazy val client = MinioClient
    .builder()
    .endpoint(MINIO_ENDPOINT)
    .credentials(S3_ACCESS_KEY, S3_SECRET_KEY)
    .build()

  def bucketExists(bucketName: String): Boolean = {
    client.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())
  }

  def clearBucket(bucketName: String): Unit = {
    val results = client.listObjects(
      ListObjectsArgs
        .builder()
        .bucket(bucketName)
        .recursive(true)
        .build())
    val objects = new util.LinkedList[DeleteObject]()
    results.forEach(obj => objects.add(new DeleteObject(obj.get().objectName())))
    val removeResults = client
      .removeObjects(
        RemoveObjectsArgs
          .builder()
          .bucket(bucketName)
          .objects(objects)
          .build())
    removeResults.forEach(result => result.get().message())
    client.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build())
  }

  def createBucket(bucketName: String): Unit = {
    client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build())
  }

  def listObjects(bucketName: String, prefix: String): Iterable[String] = {
    val args = ListObjectsArgs.builder().bucket(bucketName).recursive(true).prefix(prefix).build()
    val objectNames = mutable.ArrayBuffer[String]()
    client.listObjects(args).forEach(obj => objectNames.append(obj.get().objectName()))
    objectNames
  }

  def setFileSystem(conf: SparkConf): SparkConf = {
    conf
      .set("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
      .set("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .set("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
      .set("spark.hadoop.fs.s3a.path.style.access", "true")
      .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
  }

  def builder(policyName: String): StoreConfigBuilder =
    new StoreConfigBuilder(policyName, GlutenObjectStorageConfig.S3_DISK_TYPE)

  def setStoreConfig(conf: SparkConf, BUCKET_NAME: String): SparkConf = {
    builder(STORE_POLICY)
      .withEndpoint(s"$MINIO_ENDPOINT$BUCKET_NAME/")
      .withMetadataPath(S3_METADATA_PATH)
      .withCachePath(S3_CACHE_PATH)
      .withAKSK(S3_ACCESS_KEY, S3_SECRET_KEY)
      .build(conf)

    builder(STORE_POLICY_NOCACHE)
      .withEndpoint(s"$MINIO_ENDPOINT$BUCKET_NAME/")
      .withMetadataPath(S3_METADATA_PATH)
      .withDiskcache(false)
      .withAKSK(S3_ACCESS_KEY, S3_SECRET_KEY)
      .build(conf)
  }

  def resetMeta(): Unit = {
    FileUtils.deleteDirectory(new File(S3_METADATA_PATH))
    FileUtils.forceMkdir(new File(S3_METADATA_PATH))
  }
}
