package org.apache.spark.sql

import org.apache.spark.sql.types.{DataType, DecimalType}

object TypeUtils {

  def typeAccepts(t: DataType, other: DataType): Boolean = {
    t.acceptsType(other)
  }

  def decimalAccepts(other: DataType): Boolean = {
    DecimalType.acceptsType(other)
  }
}
