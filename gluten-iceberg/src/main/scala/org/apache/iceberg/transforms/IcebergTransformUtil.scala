package org.apache.iceberg.transforms

import org.apache.gluten.proto.{IcebergPartitionField, TransformType}
import org.apache.iceberg.PartitionField

object IcebergTransformUtil {

  def convertPartitionField(field: PartitionField): IcebergPartitionField = {
    val transform = field.transform()
    // TODO: if the field is in nest column, concat it.
    var builder = IcebergPartitionField.newBuilder().setName(field.name())
    builder = transform match {
      case _: Identity[_] => builder.setTransform(TransformType.IDENTITY)
      case _: Years[_] => builder.setTransform(TransformType.YEAR)
      case _: Months[_] => builder.setTransform(TransformType.MONTH)
      case _: Days[_] => builder.setTransform(TransformType.DAY)
      case _: Hours[_] => builder.setTransform(TransformType.HOUR)
      case b: Bucket[_] => builder.setTransform(TransformType.BUCKET).setParameter(b.numBuckets())
      case t: Truncate[_] => builder.setTransform(TransformType.TRUNCATE).setParameter(t.width)
    }
    builder.build()
  }
}
