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

package com.intel.oap.sql.execution

import java.util.concurrent.TimeUnit._

import com.intel.oap.vectorized._

import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils.UnsafeItr
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

class RowToColumnConverter(schema: StructType) extends Serializable {
  private val converters = schema.fields.map {
    f => RowToColumnConverter.getConverterForType(f.dataType, f.nullable)
  }

  final def convert(row: InternalRow, vectors: Array[WritableColumnVector]): Unit = {
    var idx = 0
    while (idx < row.numFields) {
      converters(idx).append(row, idx, vectors(idx))
      idx += 1
    }
  }
}

object RowToColumnConverter {
  private abstract class TypeConverter extends Serializable {
    def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit
  }

  private final case class BasicNullableTypeConverter(base: TypeConverter) extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      if (row.isNullAt(column)) {
        cv.appendNull
      } else {
        base.append(row, column, cv)
      }
    }
  }

  private final case class StructNullableTypeConverter(base: TypeConverter) extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      if (row.isNullAt(column)) {
        cv.appendStruct(true)
      } else {
        base.append(row, column, cv)
      }
    }
  }

  private def getConverterForType(dataType: DataType, nullable: Boolean): TypeConverter = {
    val core = dataType match {
      case BooleanType => BooleanConverter
      case ByteType => ByteConverter
      case ShortType => ShortConverter
      case IntegerType | DateType => IntConverter
      case FloatType => FloatConverter
      case LongType | TimestampType => LongConverter
      case DoubleType => DoubleConverter
      case StringType => StringConverter
      case BinaryType => BinaryConverter
      case CalendarIntervalType => CalendarConverter
      case at: ArrayType => new ArrayConverter(getConverterForType(at.elementType, nullable))
      case st: StructType => new StructConverter(st.fields.map(
        (f) => getConverterForType(f.dataType, f.nullable)))
      case dt: DecimalType => new DecimalConverter(dt)
      case mt: MapType => new MapConverter(getConverterForType(mt.keyType, nullable),
        getConverterForType(mt.valueType, nullable))
      case unknown => throw new UnsupportedOperationException(
        s"Type $unknown not supported")
    }

    if (nullable) {
      dataType match {
        case CalendarIntervalType => new StructNullableTypeConverter(core)
        case st: StructType => new StructNullableTypeConverter(core)
        case _ => new BasicNullableTypeConverter(core)
      }
    } else {
      core
    }
  }

  private object BooleanConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendBoolean(row.getBoolean(column))
  }

  private object ByteConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendByte(row.getByte(column))
  }

  private object ShortConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendShort(row.getShort(column))
  }

  private object IntConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendInt(row.getInt(column))
  }

  private object FloatConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendFloat(row.getFloat(column))
  }

  private object LongConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendLong(row.getLong(column))
  }

  private object DoubleConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit =
      cv.appendDouble(row.getDouble(column))
  }

  private object StringConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val data = row.getUTF8String(column).getBytes
      cv.asInstanceOf[ArrowWritableColumnVector].appendString(data, 0, data.length)
    }
  }

  private object BinaryConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val data = row.getBinary(column)
      cv.asInstanceOf[ArrowWritableColumnVector].appendString(data, 0, data.length)
    }
  }

  private object CalendarConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val c = row.getInterval(column)
      cv.appendStruct(false)
      cv.getChild(0).appendInt(c.months)
      cv.getChild(1).appendInt(c.days)
      cv.getChild(2).appendLong(c.microseconds)
    }
  }

  private case class ArrayConverter(childConverter: TypeConverter) extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val values = row.getArray(column)
      val numElements = values.numElements()
      cv.appendArray(numElements)
      val arrData = cv.arrayData()
      for (i <- 0 until numElements) {
        childConverter.append(values, i, arrData)
      }
    }
  }

  private case class StructConverter(childConverters: Array[TypeConverter]) extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      cv.appendStruct(false)
      val data = row.getStruct(column, childConverters.length)
      for (i <- 0 until childConverters.length) {
        childConverters(i).append(data, i, cv.getChild(i))
      }
    }
  }

  private case class DecimalConverter(dt: DecimalType) extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val d = row.getDecimal(column, dt.precision, dt.scale)
      if (dt.precision <= Decimal.MAX_INT_DIGITS) {
        cv.appendInt(d.toUnscaledLong.toInt)
      } else if (dt.precision <= Decimal.MAX_LONG_DIGITS) {
        cv.appendLong(d.toUnscaledLong)
      } else {
        val value = d.toJavaBigDecimal
        cv.asInstanceOf[ArrowWritableColumnVector].appendDecimal(value)
      }
    }
  }

  private case class MapConverter(keyConverter: TypeConverter, valueConverter: TypeConverter)
    extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      val m = row.getMap(column)
      val keys = cv.getChild(0)
      val values = cv.getChild(1)
      val numElements = m.numElements()
      cv.appendArray(numElements)

      val srcKeys = m.keyArray()
      val srcValues = m.valueArray()

      for (i <- 0 until numElements) {
        keyConverter.append(srcKeys, i, keys)
        valueConverter.append(srcValues, i, values)
      }
    }
  }
}

/**
 * Provides a common executor to translate an [[RDD]] of [[InternalRow]] into an [[RDD]] of
 * [[ColumnarBatch]]. This is inserted whenever such a transition is determined to be needed.
 *
 * This is similar to some of the code in ArrowConverters.scala and
 * [[org.apache.spark.sql.execution.arrow.ArrowWriter]]. That code is more specialized
 * to convert [[InternalRow]] to Arrow formatted data, but in the future if we make
 * [[OffHeapColumnVector]] internally Arrow formatted we may be able to replace much of that code.
 *
 * This is also similar to
 * [[org.apache.spark.sql.execution.vectorized.ColumnVectorUtils.populate()]] and
 * [[org.apache.spark.sql.execution.vectorized.ColumnVectorUtils.toBatch()]] toBatch is only ever
 * called from tests and can probably be removed, but populate is used by both Orc and Parquet
 * to initialize partition and missing columns. There is some chance that we could replace
 * populate with [[RowToColumnConverter]], but the performance requirements are different and it
 * would only be to reduce code.
 */
case class RowToArrowColumnarExec(child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.executeBroadcast()
  }

  override def supportsColumnar: Boolean = true

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_rowtoarrowcolumnar")
  )

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric("numInputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val processTime = longMetric("processTime")
    // Instead of creating a new config we are reusing columnBatchSize. In the future if we do
    // combine with some of the Arrow conversion tools we will need to unify some of the configs.
    val numRows = conf.columnBatchSize
    // This avoids calling `schema` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localSchema = this.schema
    child.execute().mapPartitions { rowIterator =>
      if (rowIterator.hasNext) {
        val res = new Iterator[ColumnarBatch] {
          private val converters = new RowToColumnConverter(localSchema)
          private var last_cb: ColumnarBatch = null
          private var elapse: Long = 0

          override def hasNext: Boolean = {
            rowIterator.hasNext
          }

          override def next(): ColumnarBatch = {
            val vectors: Seq[WritableColumnVector] =
              ArrowWritableColumnVector.allocateColumns(numRows, schema)
            var rowCount = 0
            while (rowCount < numRows && rowIterator.hasNext) {
              val row = rowIterator.next()
              val start = System.nanoTime()
              converters.convert(row, vectors.toArray)
              elapse += System.nanoTime() - start
              rowCount += 1
            }
            vectors.foreach(v => v.asInstanceOf[ArrowWritableColumnVector].setValueCount(rowCount))
            processTime.set(NANOSECONDS.toMillis(elapse))
            numInputRows += rowCount
            numOutputBatches += 1
            last_cb = new ColumnarBatch(vectors.toArray, rowCount)
            last_cb
          }
        }
        new UnsafeItr(res)
      } else {
        Iterator.empty
      }
    }
  }
}
