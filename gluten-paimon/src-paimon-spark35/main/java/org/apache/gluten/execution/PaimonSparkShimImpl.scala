package org.apache.gluten.execution

import org.apache.paimon.data.InternalRow
import org.apache.paimon.io.DataFileMeta
import org.apache.paimon.spark.PaimonScan
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.utils.InternalRowPartitionComputer
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils

import scala.collection.JavaConverters.asScalaBufferConverter

class PaimonSparkShimImpl extends PaimonSparkShim {

  override def isChainSplit(split: DataSplit): Boolean = {
    false
  }

  override def getSplitPartition(split: DataSplit): InternalRow = {
    split.partition()
  }

  override def getBucketPath(split: DataSplit, file: DataFileMeta): String = {
    split.bucketPath()
  }

  override def getInternalPartitionComputer(paimonScan: PaimonScan): InternalRowPartitionComputer = {
        val table = paimonScan.table.asInstanceOf[FileStoreTable]
        new InternalRowPartitionComputer(
          ExternalCatalogUtils.DEFAULT_PARTITION_NAME, // use __HIVE_DEFAULT_PARTITION__ because velox uses this
          table.schema().logicalPartitionType(),
          table.partitionKeys.asScala.toArray,
          false)
  }
}
