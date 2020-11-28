package io.xydrolase.spark.npy


import scala.collection.JavaConverters._
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class NumpyTable(name: String,
                      sparkSession: SparkSession,
                      options: CaseInsensitiveStringMap,
                      paths: Seq[String],
                      userSpecifiedSchema: Option[StructType],
                      fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  override def newScanBuilder(options: CaseInsensitiveStringMap): NumpyScanBuilder =
    new NumpyScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] =
    NumpyUtils.inferSchema(sparkSession, options.asScala.toMap, files)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new NumpyWriteBuilder(paths, formatName, supportsDataType, info)

  override def supportsDataType(dataType: DataType): Boolean = NumpyUtils.supportsDataType(dataType)

  override def formatName: String = "NUMPY"
}
