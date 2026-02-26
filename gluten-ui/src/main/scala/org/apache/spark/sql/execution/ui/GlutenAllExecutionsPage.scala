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
package org.apache.spark.sql.execution.ui

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.sql.catalyst.util.StringUtils.PlanStringConcat
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.ui.TypeAlias._
import org.apache.spark.status.{ElementTrackingStore, ExecutorSummaryWrapper}
import org.apache.spark.status.api.v1.ExecutorSummary
import org.apache.spark.ui.{PagedDataSource, PagedTable, UIUtils, WebUIPage}
import org.apache.spark.util.Utils

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.mutable
import scala.xml.{Node, NodeSeq}

private[ui] class GlutenAllExecutionsPage(parent: GlutenSQLTab) extends WebUIPage("") with Logging {

  private val sqlStore = parent.sqlStore

  override def render(request: HttpServletRequest): Seq[Node] = {
    val buildInfo = sqlStore.buildInfo()
    val data = sqlStore.executionsList()

    val content = {
      val _content = mutable.ListBuffer[Node]()

      val glutenPageTable =
        executionsTable(request, "gluten", data)

      _content ++=
        <span id="gluten" class="collapse-aggregated-runningExecutions collapse-table"
              onClick="collapseTable('collapse-aggregated-runningExecutions',
              'aggregated-runningExecutions')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a href="#gluten">
              Queries:
            </a>{data.size}
          </h4>
        </span> ++
          <div class="aggregated-runningExecutions collapsible-table">
            {glutenPageTable}
          </div>

      // Build Executors overview and native stack collection controls
      val execSection: Seq[Node] =
        try {
          val ui = parent.parent
          val kvStore = ui.store.store.asInstanceOf[ElementTrackingStore]
          val wrappers = Utils
            .tryWithResource(kvStore.view(classOf[ExecutorSummaryWrapper]).closeableIterator()) {
              iter =>
                import scala.collection.JavaConverters.asScalaIteratorConverter
                iter.asScala.toList
            }
          val summaries: Seq[ExecutorSummary] = wrappers.map(_.info)

          val maybeExec = Option(request.getParameter("executorId"))
          val dumpResult: Seq[Node] = maybeExec
            .map {
              id =>
                try {
                  val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv
                  val driverRef: RpcEndpointRef = parent.glutenDriverEndpointRef(rpcEnv)
                  val startMsgClass = Class.forName(
                    "org.apache.spark.rpc.GlutenRpcMessages$GlutenStartNativeStackAsync")
                  val startMsg = startMsgClass.getConstructors.head
                    .newInstance(id)
                    .asInstanceOf[AnyRef]
                  val requestId = driverRef.askSync[String](startMsg)
                  val base = UIUtils.prependBaseUri(request, parent.basePath)
                  val statusUrl = s"$base/gluten/stackStatus?requestId=$requestId"
                  Seq(<script>{
                    scala.xml.Unparsed(s"window.location.href='" + statusUrl + "';")
                  }</script>)
                } catch {
                  case t: Throwable =>
                    logWarning("Dump native stack failed", t)
                    Seq(<div class="alert alert-error"><pre>{t.getMessage}</pre></div>)
                }
            }
            .getOrElse(Nil)

          val search =
            Option(request.getParameter("glutenExec.search")).map(_.trim).filter(_.nonEmpty)
          val execPage = Option(request.getParameter("glutenExec.page")).map(_.toInt).getOrElse(1)
          val parameterPath = s"${UIUtils.prependBaseUri(request, parent.basePath)}/gluten/"

          val execTable = new GlutenExecutorsPagedTable(
            request,
            parent,
            summaries,
            tableHeaderId = "gluten-executors",
            executionTag = "glutenExec",
            basePath = UIUtils.prependBaseUri(request, parent.basePath),
            subPath = "gluten",
            searchText = search
          ).table(execPage)

          Seq(
            <span id="gluten-executors" class="collapse-aggregated-runningExecutions collapse-table"
                onClick="collapseTable('collapse-aggregated-executors','aggregated-executors')">
            <h4>
              <span class="collapse-table-arrow arrow-open"></span>
              <a href="#gluten-executors">Executors</a> {summaries.size}
            </h4>
          </span>,
            <div class="aggregated-executors collapsible-table">
              <div style="display:flex; justify-content:space-between; align-items:center; gap:10px; flex-wrap:wrap; margin-bottom:10px;">
                <form class="form-inline" action={parameterPath} method="GET">
                  <label style="margin-right:5px;">Show</label>
                  <select name="glutenExec.pageSize" class="form-control input-sm" onchange="this.form.submit()">
                    <option value="10">10</option>
                    <option value="20">20</option>
                    <option value="50">50</option>
                    <option value="100">100</option>
                  </select>
                  <span style="margin-left:5px;">entries</span>
                </form>
                <form class="form-inline" action={
              parameterPath
            } method="GET" style="margin-left:auto;">
                  <label style="margin-right:5px;">Search:</label>
                  <input type="text" name="glutenExec.search" value={
              search.getOrElse("")
            } placeholder="id/host/status" class="form-control input-sm"/>
                </form>
              </div>
            {dumpResult ++ execTable}
          </div>
          )
        } catch {
          case t: Throwable =>
            logWarning("Render Gluten Executors section failed", t)
            Seq(<div class="alert alert-warning">Failed to render Executors overview: {
              t.getMessage
            }</div>)
        }

      _content ++= execSection

      _content
    }
    content ++=
      <script>
        function clickDetail(details) {{
          details.parentNode.querySelector('.stage-details').classList.toggle('collapsed')
        }}
      </script>

    val infos = UIUtils.listingTable(
      propertyHeader,
      propertyRow,
      buildInfo.info,
      fixedWidth = true
    )
    val summary: NodeSeq =
      <div>
        <div>
          <span class="collapse-sql-properties collapse-table"
                onClick="collapseTable('collapse-sql-properties', 'sql-properties')">
            <h4>
              <span class="collapse-table-arrow arrow-closed"></span>
              <a>Gluten Build Information</a>
            </h4>
          </span>
          <div class="sql-properties collapsible-table collapsed">
            {infos}
          </div>
        </div>
      </div>

    UIUtils.headerSparkPage(request, "Gluten SQL / DataFrame", summary ++ content, parent)
  }

  private def propertyHeader = Seq("Name", "Value")

  private def propertyRow(kv: (String, String)) = <tr>
    <td>
      {kv._1}
    </td> <td>
      {kv._2}
    </td>
  </tr>

  private def executionsTable(
      request: HttpServletRequest,
      executionTag: String,
      executionData: Seq[GlutenSQLExecutionUIData]): Seq[Node] = {

    val executionPage =
      Option(request.getParameter(s"$executionTag.page")).map(_.toInt).getOrElse(1)

    val tableHeaderId = executionTag

    try {
      new GlutenExecutionPagedTable(
        request,
        parent,
        executionData,
        tableHeaderId,
        executionTag,
        UIUtils.prependBaseUri(request, parent.basePath),
        "gluten").table(executionPage)
    } catch {
      case e @ (_: IllegalArgumentException | _: IndexOutOfBoundsException) =>
        <div class="alert alert-error">
          <p>Error while rendering execution table:</p>
          <pre>
            {Utils.exceptionString(e)}
          </pre>
        </div>
    }
  }
}

private[ui] class GlutenExecutionPagedTable(
    request: HttpServletRequest,
    parent: GlutenSQLTab,
    data: Seq[GlutenSQLExecutionUIData],
    tableHeaderId: String,
    executionTag: String,
    basePath: String,
    subPath: String)
  extends PagedTable[GlutenExecutionTableRowData] {

  private val (sortColumn, desc, pageSize) = getTableParameters(request, executionTag, "ID")

  private val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())

  override val dataSource = new GlutenExecutionDataSource(data, pageSize, sortColumn, desc)

  private val parameterPath = {
    // Encode existing query parameters to avoid invalid URIs (e.g., spaces in values)
    def encodeQuery(q: String): String = {
      if (q == null || q.isEmpty) ""
      else {
        q.split("&")
          .toSeq
          .map {
            kv =>
              val idx = kv.indexOf('=')
              if (idx >= 0) {
                val k = kv.substring(0, idx)
                val v = kv.substring(idx + 1)
                s"${URLEncoder.encode(k, UTF_8.name())}=${URLEncoder.encode(v, UTF_8.name())}"
              } else {
                URLEncoder.encode(kv, UTF_8.name())
              }
          }
          .mkString("&")
      }
    }
    val otherRaw = getParameterOtherTable(request, executionTag)
    val other = encodeQuery(otherRaw)
    val base = s"$basePath/$subPath/"
    if (other.nonEmpty) s"$base?$other" else s"$base?"
  }

  override def tableId: String = s"$executionTag-table"

  override def tableCssClass: String =
    "table table-bordered table-sm table-striped table-head-clickable table-cell-width-limited"

  override def pageLink(page: Int): String = {
    parameterPath +
      s"&$pageNumberFormField=$page" +
      s"&$executionTag.sort=$encodedSortColumn" +
      s"&$executionTag.desc=$desc" +
      s"&$pageSizeFormField=$pageSize" +
      s"#$tableHeaderId"
  }

  override def pageSizeFormField: String = s"$executionTag.pageSize"

  override def pageNumberFormField: String = s"$executionTag.page"

  override def goButtonFormPath: String =
    s"$parameterPath&$executionTag.sort=$encodedSortColumn&$executionTag.desc=$desc#$tableHeaderId"

  // Information for each header: title, sortable, tooltip
  private val headerInfo: Seq[(String, Boolean, Option[String])] = {
    Seq(
      ("ID", true, None),
      ("Description", true, None),
      ("Num Gluten Nodes", true, None),
      ("Num Fallback Nodes", true, None))
  }

  override def headers: Seq[Node] = {
    isSortColumnValid(headerInfo, sortColumn)

    headerRow(headerInfo, desc, pageSize, sortColumn, parameterPath, executionTag, tableHeaderId)
  }

  override def row(executionTableRow: GlutenExecutionTableRowData): Seq[Node] = {
    val executionUIData = executionTableRow.executionUIData

    <tr>
      <td>
        {executionUIData.executionId.toString}
      </td>
      <td>
        {descriptionCell(executionUIData)}
      </td>
      <td sorttable_customkey={executionUIData.numGlutenNodes.toString}>
        {executionUIData.numGlutenNodes.toString}
      </td>
      <td sorttable_customkey={executionUIData.numFallbackNodes.toString}>
        {executionUIData.numFallbackNodes.toString}
      </td>
    </tr>
  }

  private def descriptionCell(execution: GlutenSQLExecutionUIData): Seq[Node] = {
    val details = if (execution.description != null && execution.description.nonEmpty) {
      val concat = new PlanStringConcat()
      concat.append("== Fallback Summary ==\n")
      val fallbackSummary = execution.fallbackNodeToReason
        .map {
          case (name, reason) =>
            val id = name.substring(0, 3)
            val nodeName = name.substring(4)
            s"(${id.toInt}) $nodeName: $reason"
        }
        .mkString("\n")
      concat.append(fallbackSummary)
      if (execution.fallbackNodeToReason.isEmpty) {
        concat.append("No fallback nodes")
      }
      concat.append("\n\n")
      concat.append(execution.fallbackDescription)

      <span onclick="this.parentNode.querySelector('.stage-details').classList.toggle('collapsed')"
            class="expand-details">
        +details
      </span> ++
        <div class="stage-details collapsed">
        <pre>{concat.toString()}</pre>
      </div>
    } else {
      Nil
    }

    val desc = if (execution.description != null && execution.description.nonEmpty) {
      <a href={executionURL(execution.executionId)} class="description-input">
        {execution.description}</a>
    } else {
      <a href={executionURL(execution.executionId)}>{execution.executionId}</a>
    }

    <div>{desc}{details}</div>
  }

  private def executionURL(executionID: Long): String =
    s"${UIUtils.prependBaseUri(request, parent.basePath)}/SQL/execution/?id=$executionID"
}

private[ui] class GlutenExecutionTableRowData(val executionUIData: GlutenSQLExecutionUIData)

private[ui] class GlutenExecutionDataSource(
    executionData: Seq[GlutenSQLExecutionUIData],
    pageSize: Int,
    sortColumn: String,
    desc: Boolean)
  extends PagedDataSource[GlutenExecutionTableRowData](pageSize) {

  // Convert ExecutionData to ExecutionTableRowData which contains the final contents to show
  // in the table so that we can avoid creating duplicate contents during sorting the data
  private val data = executionData.map(executionRow).sorted(ordering(sortColumn, desc))

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[GlutenExecutionTableRowData] =
    data.slice(from, to)

  private def executionRow(
      executionUIData: GlutenSQLExecutionUIData): GlutenExecutionTableRowData = {
    new GlutenExecutionTableRowData(executionUIData)
  }

  /** Return Ordering according to sortColumn and desc. */
  private def ordering(sortColumn: String, desc: Boolean): Ordering[GlutenExecutionTableRowData] = {
    val ordering: Ordering[GlutenExecutionTableRowData] = sortColumn match {
      case "ID" => Ordering.by(_.executionUIData.executionId)
      case "Description" => Ordering.by(_.executionUIData.fallbackDescription)
      case "Num Gluten Nodes" => Ordering.by(_.executionUIData.numGlutenNodes)
      case "Num Fallback Nodes" => Ordering.by(_.executionUIData.numFallbackNodes)
      case unknownColumn => throw QueryExecutionErrors.unknownColumnError(unknownColumn)
    }
    if (desc) {
      ordering.reverse
    } else {
      ordering
    }
  }
}

private[ui] class GlutenExecutorsPagedTable(
    request: HttpServletRequest,
    parent: GlutenSQLTab,
    data: Seq[ExecutorSummary],
    tableHeaderId: String,
    executionTag: String,
    basePath: String,
    subPath: String,
    searchText: Option[String])
  extends PagedTable[ExecutorSummary] {

  private val (sortColumn, desc, pageSize) = getTableParameters(request, executionTag, "ID")
  // Use space-free internal sort keys to avoid invalid URI query parameters
  private def sortKeyFor(name: String): String = name match {
    case "ID" => "ID"
    case "Address" => "Address"
    case "Status" => "Status"
    case "Active Tasks" => "Active_Tasks"
    case "Failed Tasks" => "Failed_Tasks"
    case "Completed Tasks" => "Completed_Tasks"
    case "Total Tasks" => "Total_Tasks"
    case "Total GC Time" => "Total_GC_Time"
    case "Total Input Bytes" => "Total_Input_Bytes"
    case "Total Shuffle Read" => "Total_Shuffle_Read"
    case "Total Shuffle Write" => "Total_Shuffle_Write"
    case other => other.replace(' ', '_')
  }
  private def displayNameForKey(key: String): String = key match {
    case "Active_Tasks" => "Active Tasks"
    case "Failed_Tasks" => "Failed Tasks"
    case "Completed_Tasks" => "Completed Tasks"
    case "Total_Tasks" => "Total Tasks"
    case "Total_GC_Time" => "Total GC Time"
    case "Total_Input_Bytes" => "Total Input Bytes"
    case "Total_Shuffle_Read" => "Total Shuffle Read"
    case "Total_Shuffle_Write" => "Total Shuffle Write"
    case other => other
  }
  private val encodedSortColumn = URLEncoder.encode(sortKeyFor(sortColumn), UTF_8.name())

  private val filtered: Seq[ExecutorSummary] = searchText match {
    case Some(q) =>
      val qq = q.toLowerCase
      data.filter {
        s =>
          s.id.toLowerCase.contains(qq) ||
          Option(s.hostPort).exists(_.toLowerCase.contains(qq)) ||
          (if (s.isActive) "active" else "dead").contains(qq)
      }
    case None => data
  }

  override val dataSource = new PagedDataSource[ExecutorSummary](pageSize) {
    private val sorted = filtered.sorted(ordering(sortColumn, desc))
    override def dataSize: Int = sorted.size
    override def sliceData(from: Int, to: Int): Seq[ExecutorSummary] = sorted.slice(from, to)
  }

  private val parameterPath = {
    // Encode existing query parameters to avoid invalid URIs (e.g., spaces in values)
    def encodeQuery(q: String): String = {
      if (q == null || q.isEmpty) ""
      else {
        q.split("&")
          .toSeq
          .map {
            kv =>
              val idx = kv.indexOf('=')
              if (idx >= 0) {
                val k = kv.substring(0, idx)
                val v = kv.substring(idx + 1)
                s"${URLEncoder.encode(k, UTF_8.name())}=${URLEncoder.encode(v, UTF_8.name())}"
              } else {
                URLEncoder.encode(kv, UTF_8.name())
              }
          }
          .mkString("&")
      }
    }
    val other = encodeQuery(getParameterOtherTable(request, executionTag))
    val search = searchText
      .map(s => s"$executionTag.search=${URLEncoder.encode(s, UTF_8.name())}")
      .getOrElse("")
    val base = s"$basePath/$subPath/"
    val query =
      if (other.nonEmpty && search.nonEmpty) s"$other&$search"
      else if (other.nonEmpty) other
      else search
    if (query.nonEmpty) s"$base?$query" else s"$base?"
  }

  override def tableId: String = s"$executionTag-table"
  override def tableCssClass: String =
    "table table-bordered table-sm table-striped table-head-clickable table-cell-width-limited"

  override def pageLink(page: Int): String = {
    val sep = if (parameterPath.endsWith("?")) "" else "&"
    parameterPath +
      s"$sep$pageNumberFormField=$page" +
      s"&$executionTag.sort=$encodedSortColumn" +
      s"&$executionTag.desc=$desc" +
      s"&$pageSizeFormField=$pageSize" +
      s"#$tableHeaderId"
  }

  override def pageSizeFormField: String = s"$executionTag.pageSize"
  override def pageNumberFormField: String = s"$executionTag.page"
  override def goButtonFormPath: String = {
    val sep = if (parameterPath.endsWith("?")) "" else "&"
    s"$parameterPath$sep$executionTag.sort=$encodedSortColumn&$executionTag.desc=$desc#$tableHeaderId"
  }

  private val headerInfo: Seq[(String, Boolean, Option[String])] = Seq(
    ("ID", true, None),
    ("Address", true, None),
    ("Status", true, None),
    ("Active Tasks", true, None),
    ("Failed Tasks", true, None),
    ("Completed Tasks", true, None),
    ("Total Tasks", true, None),
    ("Total GC Time", true, None),
    ("Total Input Bytes", true, None),
    ("Total Shuffle Read", true, None),
    ("Total Shuffle Write", true, None),
    ("C++ Thread Dump", false, None)
  )

  override def headers: Seq[Node] = {
    isSortColumnValid(headerInfo, displayNameForKey(sortColumn))
    val ths: Seq[Node] = headerInfo.map {
      case (name, sortable, _) =>
        if (sortable) {
          val encodedName = URLEncoder.encode(sortKeyFor(name), UTF_8.name())
          val newDesc = if (sortKeyFor(sortColumn) == sortKeyFor(name)) !desc else false
          val sep = if (parameterPath.endsWith("?")) "" else "&"
          val href = parameterPath +
            s"$sep$pageNumberFormField=1" +
            s"&$executionTag.sort=$encodedName" +
            s"&$executionTag.desc=$newDesc" +
            s"&$pageSizeFormField=$pageSize" +
            s"#$tableHeaderId"
          <th><a href={href}>{name}</a></th>
        } else {
          <th>{name}</th>
        }
    }
    Seq(<thead><tr>{ths}</tr></thead>)
  }

  override def row(s: ExecutorSummary): Seq[Node] = {
    val link = UIUtils.prependBaseUri(request, parent.basePath) + s"/gluten/?executorId=${s.id}"
    val statusText = if (s.isActive) "ACTIVE" else "DEAD"
    def fmtBytes(v: Long): String = {
      val abs = math.abs(v)
      if (abs < 1024L) s"${v}B"
      else if (abs < 1024L * 1024L) f"${v / 1024.0}%.1fK"
      else if (abs < 1024L * 1024L * 1024L) f"${v / (1024.0 * 1024)}%.1fM"
      else f"${v / (1024.0 * 1024 * 1024)}%.1fG"
    }
    <tr>
      <td>{s.id}</td>
      <td>{s.hostPort}</td>
      <td>{statusText}</td>
      <td sorttable_customkey={s.activeTasks.toString}>{s.activeTasks.toString}</td>
      <td sorttable_customkey={s.failedTasks.toString}>{s.failedTasks.toString}</td>
      <td sorttable_customkey={s.completedTasks.toString}>{s.completedTasks.toString}</td>
      <td sorttable_customkey={s.totalTasks.toString}>{s.totalTasks.toString}</td>
      <td sorttable_customkey={s.totalGCTime.toString}>{s.totalGCTime.toString}</td>
      <td sorttable_customkey={s.totalInputBytes.toString}>{fmtBytes(s.totalInputBytes)}</td>
      <td sorttable_customkey={s.totalShuffleRead.toString}>{fmtBytes(s.totalShuffleRead)}</td>
      <td sorttable_customkey={s.totalShuffleWrite.toString}>{fmtBytes(s.totalShuffleWrite)}</td>
      {
        // Hide C++ stack link for driver executor
        if (s.id == "driver") {
          <td></td>
        } else {
          <td><a href={link}>C++ Stack</a></td>
        }
      }
    </tr>
  }

  private def ordering(sortColumn: String, desc: Boolean): Ordering[ExecutorSummary] = {
    val key = sortColumn match {
      case "Num_Gluten_Nodes" => "Num Gluten Nodes"
      case "Num_Fallback_Nodes" => "Num Fallback Nodes"
      case "Active_Tasks" => "Active Tasks"
      case "Failed_Tasks" => "Failed Tasks"
      case "Completed_Tasks" => "Completed Tasks"
      case "Total_Tasks" => "Total Tasks"
      case "Total_GC_Time" => "Total GC Time"
      case "Total_Input_Bytes" => "Total Input Bytes"
      case "Total_Shuffle_Read" => "Total Shuffle Read"
      case "Total_Shuffle_Write" => "Total Shuffle Write"
      case other => other
    }
    val ord: Ordering[ExecutorSummary] = key match {
      case "ID" => Ordering.by(_.id)
      case "Address" => Ordering.by(_.hostPort)
      case "Status" => Ordering.by(s => if (s.isActive) 1 else 0)
      case "Active Tasks" => Ordering.by(_.activeTasks)
      case "Failed Tasks" => Ordering.by(_.failedTasks)
      case "Completed Tasks" => Ordering.by(_.completedTasks)
      case "Total Tasks" => Ordering.by(_.totalTasks)
      case "Total GC Time" => Ordering.by(_.totalGCTime)
      case "Total Input Bytes" => Ordering.by(_.totalInputBytes)
      case "Total Shuffle Read" => Ordering.by(_.totalShuffleRead)
      case "Total Shuffle Write" => Ordering.by(_.totalShuffleWrite)
      case unknown => throw QueryExecutionErrors.unknownColumnError(unknown)
    }
    if (desc) ord.reverse else ord
  }
}
