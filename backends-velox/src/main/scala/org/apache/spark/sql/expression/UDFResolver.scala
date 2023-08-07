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
package org.apache.spark.sql.expression

import io.glutenproject.backendsapi.velox.BackendSettings
import io.glutenproject.expression.{ConverterUtils, ExpressionTransformer, Transformable}
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.{ExpressionType, TypeConverter}
import io.glutenproject.substrait.expression.ExpressionBuilder
import io.glutenproject.udf.UdfJniWrapper
import io.glutenproject.vectorized.JniWorkspace

import org.apache.spark.{SparkContext, SparkFiles}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.Utils

import com.google.common.collect.Lists
import org.apache.hadoop.yarn.conf.YarnConfiguration

import java.io.File
import java.nio.file.{Files, Path, Paths}

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class UDFExpression(
    name: String,
    dataType: DataType,
    nullable: Boolean,
    children: Seq[Expression])
  extends Transformable {
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    this.copy(children = newChildren)
  }

  override def getTransformer(
      childrenTransformers: Seq[ExpressionTransformer]): ExpressionTransformer = {
    if (childrenTransformers.size != children.size) {
      throw new IllegalStateException(
        this.getClass.getSimpleName +
          ": getTransformer called before children transformer initialized.")
    }
    (args: Object) => {
      val transformers = childrenTransformers.map(_.doTransform(args))
      val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
      val functionId = ExpressionBuilder.newScalarFunction(
        functionMap,
        ConverterUtils.makeFuncName(name, children.map(_.dataType), FunctionConfig.REQ))

      val typeNode = ConverterUtils.getTypeNode(dataType, nullable)
      ExpressionBuilder.makeScalarFunction(
        functionId,
        Lists.newArrayList(transformers: _*),
        typeNode)
    }
  }
}

object UDFResolver extends Logging {
  // Cache the fetched library paths for driver.
  var localLibraryPaths: Seq[String] = Seq.empty

  private val UDFMap: mutable.Map[String, ExpressionType] = mutable.Map()

  private val LIB_EXTENSION = ".so"

  def registerUDF(name: String, bytes: Array[Byte]): Unit = {
    registerUDF(name, TypeConverter.from(bytes))
  }

  def registerUDF(name: String, t: ExpressionType): Unit = {
    UDFMap.update(name, t)
    logInfo(s"Registered UDF: $name -> $t")
  }

  def parseName(name: String): (String, String) = {
    val index = name.lastIndexOf("#")
    if (index == -1) {
      (name, Paths.get(name).getFileName.toString)
    } else {
      (name.substring(0, index), name.substring(index + 1))
    }
  }

  def getFilesWithExtension(directory: Path, extension: String): Seq[Path] = {
    Files
      .walk(directory)
      .filter(p => Files.isRegularFile(p) && p.toString.endsWith(extension))
      .iterator()
      .asScala
      .toSeq
  }

  def unpackAndGetLibraries(source: File, destDir: File): Seq[String] = {
    val sourceName = source.getName
    val dest = new File(destDir, sourceName)
    logInfo(
      s"Unpacking an archive $sourceName from ${source.getAbsolutePath} to ${dest.getAbsolutePath}")
    Utils.deleteRecursively(dest)
    Utils.unpack(source, dest)
    getFilesWithExtension(dest.toPath, LIB_EXTENSION).map(_.toString)
  }

  def getAllLibraries(files: String): Seq[String] = {
    getAllLibraries(files.split(","))
  }

  def getAllLibraries(files: Seq[String]): Seq[String] = {
    files
      .map {
        f =>
          val file = new File(f)
          if (file.isAbsolute) {
            file
          } else {
            new File(SparkFiles.get(f))
          }
      }
      .flatMap {
        f =>
          if (f.isDirectory) {
            getFilesWithExtension(f.toPath, LIB_EXTENSION).map(_.toString)
          } else {
            Seq(f.toString)
          }
      }
  }

  def loadAndGetFunctionDescriptions: Seq[(FunctionIdentifier, ExpressionInfo, FunctionBuilder)] = {
    val sparkContext = SparkContext.getActive.get
    val sparkConf = sparkContext.conf
    sparkConf.getOption(BackendSettings.GLUTEN_VELOX_UDF_LIBS).map(_.split(",")) match {
      case None =>
        Seq.empty
      case Some(udfFiles) =>
        val master = sparkConf.getOption("spark.master")
        val isYarn = master.isDefined && master.get.equals("yarn")
        localLibraryPaths = if (isYarn) {
          // For Yarn-client or Yarn-cluster mode,
          // driver cannot get uploaded files via SparkFiles.get.
          // So we need to copy and unpack files for driver first.
          val hadoopConf = new YarnConfiguration(SparkHadoopUtil.newConfiguration(sparkConf))
          val toLoad: ListBuffer[String] = ListBuffer()

          // Handle absolute paths
          toLoad ++= udfFiles.filter(Paths.get(_).isAbsolute)

          sparkConf.getOption("spark.yarn.dist.files").foreach {
            files =>
              files.split(",").map(parseName).foreach {
                case (fullPath, nameOrAlias) =>
                  if (udfFiles.contains(nameOrAlias)) {
                    toLoad += Utils
                      .fetchFile(
                        fullPath,
                        new File(JniWorkspace.getDefault.getWorkDir),
                        sparkConf,
                        hadoopConf,
                        System.currentTimeMillis,
                        useCache = false)
                      .toString
                  }
              }
          }

          sparkConf.getOption("spark.yarn.dist.archives").foreach {
            archives =>
              archives.split(",").map(parseName).foreach {
                case (fullPath, nameOrAlias) =>
                  if (udfFiles.contains(nameOrAlias)) {
                    val source = Utils
                      .fetchFile(
                        fullPath,
                        Utils.createTempDir(),
                        sparkConf,
                        hadoopConf,
                        System.currentTimeMillis,
                        useCache = false)
                    toLoad ++= unpackAndGetLibraries(
                      source,
                      new File(JniWorkspace.getDefault.getWorkDir))
                  }
              }
          }
          toLoad
        } else {
          // Get the full paths of all libraries. Archives are already unpacked.
          getAllLibraries(udfFiles)
        }

        val allLibs = localLibraryPaths.mkString(",")
        logInfo(s"Loading UDF libraries from paths: $allLibs")
        new UdfJniWrapper().nativeLoadUdfLibraries(allLibs)

        UDFMap.map {
          case (name, t) =>
            (
              new FunctionIdentifier(name),
              new ExpressionInfo(classOf[UDFExpression].getName, name),
              (e: Seq[Expression]) => UDFExpression(name, t.dataType, t.nullable, e))
        }.toSeq
    }
  }
}
