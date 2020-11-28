package io.xydrolase.spark.npy

import java.nio.{ ByteBuffer, ByteOrder => JByteOrder }

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types._

class NumpyOutputWriterFactory(catalystSchema: StructType, options: Map[String, String]) extends OutputWriterFactory {
  @transient private lazy val numpySchema: Struct = NumpyUtils.structTypeToStruct(catalystSchema, options)

  override def getFileExtension(context: TaskAttemptContext): String = ".npy"

  override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
    new NumpyOutputWriter(path, context, catalystSchema, numpySchema)
  }
}

/**
 * Writer to convert a given [[org.apache.spark.sql.DataFrame]] with provided `schema` and data rows into output
 * structured Numpy array files.
 *
 * @param path
 * @param context
 * @param schema
 * @param numpySchema
 */
class NumpyOutputWriter(path: String, context: TaskAttemptContext,
                        schema: StructType, numpySchema: Struct) extends OutputWriter {
  // an internal counter to keep track of the number of rows written to the output stream;
  // this number is important because the Numpy file header must have the correct shape.
  private var numRowsWritten: Long = 0L

  private val rowByteBuffer = ByteBuffer.allocate(numpySchema.itemSize).order(JByteOrder.nativeOrder())
  // a temporary file where we write the "naked" numpy file data (i.e. without header.)
  private val tempFilePath = new Path(path + ".temp")
  private val tempFileOutputStream = tempFilePath.getFileSystem(context.getConfiguration).create(tempFilePath)

  override def write(row: InternalRow): Unit = {
    rowByteBuffer.clear()
    numpySchema.encode(rowByteBuffer, row)
    tempFileOutputStream.write(rowByteBuffer.array())

    numRowsWritten += 1
  }

  override def close(): Unit = {
    // when closing the file, we need to recreate the final form of the .npy file, write the header,
    // then copy the content we write to the temporary file.
    tempFileOutputStream.close()

    val outputPath = new Path(path)
    val fs = outputPath.getFileSystem(context.getConfiguration)
    val outputStream = fs.create(outputPath)
    val npyDataStream = fs.open(tempFilePath)

    // write header
    outputStream.write(NumpyUtils.createHeader(numpySchema.describe, numRowsWritten))

    // copy the npy data content from the temporary file to the final output stream.
    IOUtils.copy(npyDataStream, outputStream)
    outputStream.close()

    // delete the temporary file
    fs.delete(tempFilePath, false)
  }
}
