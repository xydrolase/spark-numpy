package io.xydrolase.spark.npy

import java.nio.ByteBuffer
import java.nio.{ ByteOrder => JByteOrder }

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DoubleType, FloatType, IntegerType, LongType, Metadata, StringType, StructType}

import scala.util.Try

object NumpyUtils {
  val NpyMagicBytes: Seq[Byte] = Seq[Byte](-109, 'N', 'U', 'M', 'P', 'Y')
  val NpyMajorVersion: Byte = 2
  val NpyMinorVersion: Byte = 0

  val PrimitiveTypes: Set[DataType] = Set(IntegerType, LongType, FloatType, DoubleType, BooleanType, StringType)

  def supportsDataType(dataType: DataType): Boolean = dataType match {
    case StringType => true
    case primitive if PrimitiveTypes.contains(primitive) => true
    case ArrayType(elementType, _) => supportsDataType(elementType)
    case StructType(fields) => fields.forall(f => supportsDataType(f.dataType))
    case _ => false
  }

  def dataTypeToDType(dataType: DataType, path: Seq[String])
                     (implicit config: NumpyOptions): DType = dataType match {
    case BooleanType => DType.bool
    case IntegerType => DType.int32
    case LongType => DType.int64
    case FloatType => DType.float32
    case DoubleType => DType.float64
    case StringType =>
      // numpy must use fixed-size string
      val maxSize = config.maxArraySize.foldLeft(
        Try(config.maximumSizeOf(path))
          .toOption.flatten.getOrElse(config.defaultStringSize)
      ) { case (cappedSize, size) => Math.min(size, cappedSize) }

      DType(ByteString(maxSize))
    // TODO: support array of string?
    case ArrayType(tpe, _) if PrimitiveTypes.contains(tpe) || tpe.isInstanceOf[StructType] =>
      val maxSize = config.maxArraySize.foldLeft(
        Try(config.maximumSizeOf(path))
          .toOption.flatten.getOrElse(config.defaultArraySize)
      ) { case (cappedSize, size) => Math.min(size, cappedSize) }

      dataTypeToDType(tpe, path).broadcast(maxSize :: Nil)
    // TODO: support Map as 2-tuple?
    case struct: StructType =>
      DType(structTypeToStruct(struct, path))
    case other =>
      throw new IllegalArgumentException(s"Unsupported DataType $other")
  }

  def structTypeToStruct(schema: StructType, options: Map[String, String]): Struct = {
    structTypeToStruct(schema, Vector.empty)(new NumpyOptions(options))
  }

  private def structTypeToStruct(schema: StructType, path: Seq[String])
                                (implicit config: NumpyOptions): Struct = {
    Struct(schema.fields.toList.map { field =>
      val dtype = dataTypeToDType(field.dataType, path :+ field.name)
      dtype.withName(field.name)
    }, schema)
  }

  def inferSchema(spark: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = {
    None
  }

  /**
   * Create a Numpy array header based on the format specification.
   *
   * @see https://numpy.org/neps/nep-0001-npy-format.html
   * @param dTypeDescr The data type (dtype) description.
   * @param numRows Number of rows written or to be written.
   * @return The file header, as bytes.
   */
  def createHeader(dTypeDescr: String, numRows: Long): Array[Byte] = {
    // NPY header in Python dict literal to describe data order, data type, version etc.
    // For structured array, the data is always encoded in rows, as the fields of each row must
    // be contiguous. Therefore, 'fortran_order' (column-major) is always false.
    val pyDictLiteralHeader = s"{'descr': $dTypeDescr, 'fortran_order': False, 'shape': ($numRows,)}".getBytes

    // 2 = majorVersion (1 byte) + minorVersion (1 byte)
    // 4 = headerSize integer (4 bytes)
    val headerSize = NpyMagicBytes.length + 2 + 4 + pyDictLiteralHeader.length

    // NOTE: technically the header size should be unsigned, and thus can be 4G;
    // for simplicity, we use signed Int.MaxValue, as it's the largest length of a string anyway
    require(headerSize <= Int.MaxValue, "Header size exceeding size limit")

    // pad the header size to be a multiple of 16.
    val paddedHeaderSize = headerSize match {
      case multipleOf16 if multipleOf16 % 16 == 0 => multipleOf16 + 16
      case otherSize => math.ceil(otherSize.toDouble / 16).toInt * 16
    }

    // the header is ALWAYS in little endian
    val fileHeader = ByteBuffer.allocate(paddedHeaderSize).order(JByteOrder.LITTLE_ENDIAN)

    fileHeader.put(NpyMagicBytes.toArray)
    fileHeader.put(NpyMajorVersion)
    fileHeader.put(NpyMinorVersion)
    fileHeader.putInt(paddedHeaderSize - NpyMagicBytes.length - 6)
    fileHeader.put(pyDictLiteralHeader)

    // add padding whitespaces
    if (fileHeader.remaining() > 0) {
      fileHeader.put(Array.fill[Byte](fileHeader.remaining() - 1)(' '))
      fileHeader.put('\n'.toByte)
    }

    fileHeader.array()
  }
}
