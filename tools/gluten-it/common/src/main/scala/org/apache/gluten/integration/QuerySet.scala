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
package org.apache.gluten.integration

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import scala.collection.mutable

/** A set of SQL queries. */
case class Query(id: String, path: String, sql: String)

case class QuerySet(queryIds: Seq[String], queryMap: Map[String, Query]) {
  assert(queryIds.size == queryMap.size)
  private val queryCount = queryIds.size
  private val queryIdSet = queryIds.toSet
  require(queryIdSet.size == queryCount, s"Duplicated query IDs found in the set: $queryIds")

  val queries: Seq[Query] = queryIds.map(queryMap(_))

  def filter(filteredQueryIds: Seq[String]): QuerySet = {
    filteredQueryIds.foreach {
      qid => require(queryIdSet.contains(qid), s"Query ID $qid is not found in the set: $queryIds")
    }
    val filteredQueryIdSet = filteredQueryIds.toSet
    QuerySet(
      filteredQueryIds,
      queryMap.filter { case (qid, _) => filteredQueryIdSet.contains(qid) })
  }

  def exclude(excludedQueryIds: Seq[String]): QuerySet = {
    excludedQueryIds.foreach {
      qid => require(queryIdSet.contains(qid), s"Query ID $qid is not found in the set: $queryIds")
    }

    val excludedQueryIdSet = excludedQueryIds.toSet
    val remainingQueryIds = queryIds.filter(!excludedQueryIdSet.contains(_))
    filter(remainingQueryIds)
  }

  def getShard(shardId: Int, shardCount: Int): QuerySet = {
    val least: Int = queryCount / shardCount
    val shardStart: Int = shardId * least
    var numQueriesInShard: Int = 0
    if (shardId == shardCount - 1) {
      val remaining: Int = queryCount - least * shardCount
      numQueriesInShard = least + remaining
    } else {
      numQueriesInShard = least
    }
    val shardQueryIds = mutable.ArrayBuffer[String]()
    for (i <- shardStart until shardStart + numQueriesInShard) {
      shardQueryIds += queryIds(i)
    }
    filter(shardQueryIds.toSeq)
  }

  def getQuery(queryId: String): Query = {
    queryMap(queryId)
  }
}

object QuerySet {
  private def resourceToString(resource: String): String = {
    val inStream = QuerySet.getClass.getResourceAsStream(resource)
    require(inStream != null, s"Resource not found: $resource")
    val outStream = new ByteArrayOutputStream
    try {
      var reading = true
      while (reading) {
        inStream.read() match {
          case -1 => reading = false
          case c => outStream.write(c)
        }
      }
      outStream.flush()
    } finally {
      inStream.close()
    }
    new String(outStream.toByteArray, StandardCharsets.UTF_8)
  }

  def readFromResource(folder: String, queryIds: Seq[String]): QuerySet = {
    val queries = queryIds.map {
      qid =>
        val path = s"$folder/$qid.sql"
        val sql = resourceToString(path)
        qid -> Query(qid, path, sql)
    }.toMap
    QuerySet(queryIds, queries)
  }
}
