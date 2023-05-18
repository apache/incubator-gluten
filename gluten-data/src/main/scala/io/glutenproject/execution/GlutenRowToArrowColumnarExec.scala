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

package io.glutenproject.execution

import scala.collection.mutable.ListBuffer
import io.glutenproject.columnarbatch.GlutenColumnarBatches
import io.glutenproject.memory.alloc.NativeMemoryAllocators
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.utils.GlutenArrowAbiUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, SpecializedGetters, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.{TaskContext, broadcast}
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkSchemaUtil
import org.apache.spark.sql.utils.SparkArrowUtil
import io.glutenproject.vectorized._
import org.apache.arrow.c.{ArrowArray, ArrowSchema}
import org.apache.arrow.memory.ArrowBuf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.memory.TaskResources

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
      case at: ArrayType => new ArrayConverter(getConverterForType(at.elementType, at.containsNull))
      case st: StructType => new StructConverter(st.fields.map(
        (f) => getConverterForType(f.dataType, f.nullable)))
      case dt: DecimalType => new DecimalConverter(dt)
      case mt: MapType => new MapConverter(getConverterForType(mt.keyType, nullable = false),
        getConverterForType(mt.valueType, mt.valueContainsNull))
      case NullType => NullConverter
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

  def supportSchema(schema: StructType): Boolean = {
    try {
      schema.fields.map {
        f =>
          RowToColumnConverter.getConverterForType(f.dataType, f.nullable)
          SparkArrowUtil.toArrowType(f.dataType, SparkSchemaUtil.getLocalTimezoneID)
      }
      true
    } catch {
      case _: UnsupportedOperationException => false
      case e => throw e
    }
  }

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
      for (i <- childConverters.indices) {
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

  private object NullConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, cv: WritableColumnVector): Unit = {
      cv.asInstanceOf[ArrowWritableColumnVector].appendNull()
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
case class GlutenRowToArrowColumnarExec(child: SparkPlan)
  extends GlutenRowToColumnarExec(child = child) with UnaryExecNode {

  private def typeCheckNative(): Boolean = {
    for (field <- schema.fields) {
      field.dataType match {
        case _: BooleanType =>
        case _: ByteType =>
        case _: ShortType =>
        case _: IntegerType =>
        case _: LongType =>
        case _: FloatType =>
        case _: DoubleType =>
        case _: StringType =>
        case _: BinaryType =>
        case _: DecimalType =>
        case _: DateType =>
        case _: TimestampType =>
        case _ => return false
      }
    }
    true
  }

  override def doExecuteColumnarInternal(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric("numInputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val convertTime = longMetric("convertTime")
    // Instead of creating a new config we are reusing columnBatchSize. In the future if we do
    // combine with some of the Arrow conversion tools we will need to unify some of the configs.
    val numRows = conf.columnBatchSize
    // This avoids calling `schema` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localSchema = schema
    val useNative = typeCheckNative()
    child.execute().mapPartitions { rowIterator =>
      val converter = new RowToColumnConverter(localSchema)
      val arrowSchema = SparkArrowUtil.toArrowSchema(localSchema, SQLConf.get.sessionLocalTimeZone)
      val jniWrapper = new NativeRowToColumnarJniWrapper()
      val allocator = ArrowBufferAllocators.contextInstance()
      val cSchema = ArrowSchema.allocateNew(allocator)
      var closed = false
      val r2cId = try {
        if (useNative) {
          GlutenArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
          jniWrapper.init(cSchema.memoryAddress(),
            NativeMemoryAllocators.contextInstance().getNativeInstanceId)
        } else {
          closed = true
          -1
        }
      } finally {
        cSchema.close()
      }


      TaskResources.addRecycler(100) {
        if (!closed) {
          jniWrapper.close(r2cId)
          closed = true
        }
      }

      if (rowIterator.hasNext) {
        val res: Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {

          override def hasNext: Boolean = {
            val itHasNext = rowIterator.hasNext
            if (!itHasNext && !closed) {
              jniWrapper.close(r2cId)
              closed = true
            }
            itHasNext
          }

          def nativeConvert(row: UnsafeRow): ColumnarBatch = {
            var arrowBuf: ArrowBuf = null
            TaskResources.addRecycler(100) {
              // Remind, remove isOpen here
              if (arrowBuf != null && arrowBuf.refCnt() == 0) {
                arrowBuf.close()
              }
            }
            val rowLength = new ListBuffer[Long]()
            var rowCount = 0
            var offset = 0
            val sizeInBytes = row.getSizeInBytes
            // allocate buffer based on 1st row, but if first row is very big, this will cause OOM
            // maybe we should optimize to list ArrayBuf to native to avoid buf close and allocate
            // 31760L origins from BaseVariableWidthVector.lastValueAllocationSizeInBytes
            // experimental value
            val estimatedBufSize = Math.max(
              Math.min(sizeInBytes.toDouble * numRows * 1.2, 31760L * numRows),
              sizeInBytes.toDouble * 10)
            arrowBuf = allocator.buffer(estimatedBufSize.toLong)
            Platform.copyMemory(row.getBaseObject, row.getBaseOffset,
              null, arrowBuf.memoryAddress() + offset, sizeInBytes)
            offset += sizeInBytes
            rowLength += sizeInBytes.toLong
            rowCount += 1

            while (rowCount < numRows && rowIterator.hasNext) {
              val row = rowIterator.next()
              val unsafeRow = row.asInstanceOf[UnsafeRow]
              val sizeInBytes = unsafeRow.getSizeInBytes
              if ((offset + sizeInBytes) > arrowBuf.capacity()) {
                val tmpBuf = allocator.buffer(((offset + sizeInBytes) * 2).toLong)
                tmpBuf.setBytes(0, arrowBuf, 0, offset)
                arrowBuf.close()
                arrowBuf = tmpBuf
              }
              Platform.copyMemory(unsafeRow.getBaseObject, unsafeRow.getBaseOffset,
                null, arrowBuf.memoryAddress() + offset, sizeInBytes)
              offset += sizeInBytes
              rowLength += sizeInBytes.toLong
              rowCount += 1
            }
            numInputRows += rowCount
            try {
              val handle = jniWrapper.nativeConvertRowToColumnar(
                r2cId, rowLength.toArray,
                arrowBuf.memoryAddress())
              GlutenColumnarBatches.create(handle)
            } finally {
              arrowBuf.close()
              arrowBuf = null
            }
          }

          def javaConvert(row: InternalRow): ColumnarBatch = {
            logInfo("Not UnsafeRow, fallback to java based r2c")
            val vectors: Seq[WritableColumnVector] =
              ArrowWritableColumnVector.allocateColumns(numRows, schema)
            var rowCount = 0

            converter.convert(row, vectors.toArray)
            rowCount += 1

            while (rowCount < numRows && rowIterator.hasNext) {
              val row = rowIterator.next()
              converter.convert(row, vectors.toArray)
              rowCount += 1
            }
            vectors.foreach(v => v.asInstanceOf[ArrowWritableColumnVector]
              .setValueCount(rowCount))
            numInputRows += rowCount
            new ColumnarBatch(vectors.toArray, rowCount)
          }

          override def next(): ColumnarBatch = {
            val firstRow = rowIterator.next()
            val start = System.currentTimeMillis()
            val cb = firstRow match {
              case unsafeRow: UnsafeRow if useNative =>
                nativeConvert(unsafeRow)
              case _ =>
                javaConvert(firstRow)
            }
            numOutputBatches += 1
            convertTime += System.currentTimeMillis() - start
            cb
          }
        }
        new CloseableColumnBatchIterator(res)
      } else {
        Iterator.empty
      }
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def supportsColumnar: Boolean = true

  // For spark 3.2.
  protected def withNewChildInternal(newChild: SparkPlan): GlutenRowToArrowColumnarExec =
    copy(child = newChild)

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.executeBroadcast()
  }
}
