package io.xydrolase.spark.npy

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory}
import org.apache.spark.sql.types.{DataType, StructType}

class NumpyFileFormat extends FileFormat {
  override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = true

  override def inferSchema(sparkSession: SparkSession, options: Map[String, String],
                           files: Seq[FileStatus]): Option[StructType] = {
    NumpyUtils.inferSchema(sparkSession, options, files)
  }

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job, options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
    new NumpyOutputWriterFactory(dataSchema, options)
  }

  override def supportDataType(dataType: DataType): Boolean = NumpyUtils.supportsDataType(dataType)
}
