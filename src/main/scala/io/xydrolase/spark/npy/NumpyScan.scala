package io.xydrolase.spark.npy

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class NumpyScan(sparkSession: SparkSession,
                     fileIndex: PartitioningAwareFileIndex,
                     dataSchema: StructType,
                     readDataSchema: StructType,
                     readPartitionSchema: StructType,
                     options: CaseInsensitiveStringMap,
                     partitionFilters: Seq[Expression] = Seq.empty,
                     dataFilters: Seq[Expression] = Seq.empty) extends FileScan {
  override def isSplitable(path: Path): Boolean = true

  override def withFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan = {
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)
  }

  override def equals(obj: Any): Boolean = obj match {
    case a: NumpyScan => super.equals(a) && dataSchema == a.dataSchema && options == a.options
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

  override def createReaderFactory(): PartitionReaderFactory = ???
}
