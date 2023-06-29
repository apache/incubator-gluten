package org.apache.spark.sql.execution.datasources

import io.glutenproject.execution.{TransformSupport, WholeStageTransformer}
import io.glutenproject.execution.datasource.GlutenParquetWriterInjects
import io.glutenproject.extension.TransformPreOverrides
import io.glutenproject.extension.columnar.AddTransformHintRule

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.ColumnarCollapseTransformStages.transformStageCounter
import org.apache.spark.sql.execution.SparkPlan

trait GlutenParquetWriterInjectsBase extends GlutenParquetWriterInjects {

  /**
   * FileFormatWriter wraps some Project & Sort on the top of the original output spark plan, we
   * need to replace them with Columnar version
   * @param plan
   *   must be a FakeRowAdaptor
   * @return
   */
  override def executeWriterWrappedSparkPlan(plan: SparkPlan): RDD[InternalRow] = {
    val transformed = TransformPreOverrides(false, false).apply(AddTransformHintRule().apply(plan))
    if (!transformed.isInstanceOf[TransformSupport]) {
      throw new IllegalStateException(
        "Cannot transform the SparkPlans wrapped by FileFormatWriter, " +
          "consider disabling native parquet writer to workaround this issue.")
    }
    FakeRowAdaptor(WholeStageTransformer(transformed)(transformStageCounter.incrementAndGet()))
      .execute()
  }
}