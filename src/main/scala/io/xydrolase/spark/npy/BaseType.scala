package io.xydrolase.spark.npy

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

object BaseType {
  val ZeroByte: Byte = 0
}

/**
 * Corresponds to the "base" type of a Numpy data type (`dtype`).
 * For example, the base type of a float array is "float":
 *
 * {{{
 *   In : np.dtype(('<f4', 10)).base
 *   Out: dtype('float32')
 * }}}
 */
sealed trait BaseType extends Serializable {
  def sqlType: DataType

  /**
   * @return The number of bytes each element occupies in memory.
   */
  def itemSize: Int

  /**
   * @return Numpy-compatible description of data-type (e.g. f4 = Float/float32 etc.)
   */
  protected def descr: String

  /**
   * @return Description of data-type with byte order information.
   */
  def describe: String = s"'$describeByteOrder$descr'"

  // byte order (if applicable)
  def byteOrder: Option[ByteOrder] = Some(Native)

  /**
   * Write the binary representation of the given `value` into the designated `buffer`.
   * @param buffer
   * @param value
   */
  private[npy] def encode(buffer: ByteBuffer, value: Any): Unit

  private def describeByteOrder: Char = byteOrder.map(_.describe).getOrElse('|')
}

case object Float32 extends BaseType {
  override val sqlType: DataType = FloatType
  override val itemSize: Int = 4
  override val descr: String = "f4"

  override def encode(buffer: ByteBuffer, value: Any): Unit = {
    // since `Float` extends `AnyVal`, if the underlying `value` is null, we will simply get 0.0f
    buffer.putFloat(value.asInstanceOf[Float])
  }
}

case object Float64 extends BaseType {
  override val sqlType: DataType = DoubleType
  override val itemSize: Int = 8
  override val descr: String = "f8"

  override def encode(buffer: ByteBuffer, value: Any): Unit = {
    buffer.putDouble(value.asInstanceOf[Double])
  }
}

case object Int32 extends BaseType {
  override val sqlType: DataType = IntegerType
  override val itemSize: Int = 4
  override val descr: String = "i4"

  override def encode(buffer: ByteBuffer, value: Any): Unit = {
    buffer.putInt(value.asInstanceOf[Int])
  }
}

case object Int64 extends BaseType {
  override val sqlType: DataType = LongType
  override val itemSize: Int = 8
  override val descr: String = "i8"

  override def encode(buffer: ByteBuffer, value: Any): Unit = {
    buffer.putLong(value.asInstanceOf[Long])
  }
}

case object Bool extends BaseType {
  override val sqlType: DataType = BooleanType
  private val True: Byte = 1
  private val False: Byte = 0

  override val itemSize: Int = 1
  override val descr: String = "b1"

  override val byteOrder: Option[ByteOrder] = None

  override def encode(buffer: ByteBuffer, value: Any): Unit = {
    buffer.put(if (value.asInstanceOf[Boolean]) True else False)
  }
}

case class ByteString(length: Int) extends BaseType {
  override val sqlType: DataType = StringType
  override val itemSize: Int = length
  override val descr = s"S$length"

  override val byteOrder: Option[ByteOrder] = None

  override def encode(buffer: ByteBuffer, value: Any): Unit = {
    var bytesWritten = 0
    Option(value.asInstanceOf[String]).foreach { str =>
      val bytes = str.getBytes(StandardCharsets.UTF_8)
      bytesWritten += bytes.length
      buffer.put(bytes)
    }

    // MUST always write `length` bytes
    while (bytesWritten < length) {
      buffer.put(BaseType.ZeroByte)
      bytesWritten += 1
    }
  }
}

/**
 * Representing a C-like struct, which is a composition of a sequence of named fields.
 * Each named field is represented by anther [[DType]].
 *
 * Since [[Struct]] is a specialization of [[BaseType]], it means that the fields contained in a [[Struct]] can be
 * [[Struct]] as well.
 *
 * [[Struct]] is the counterpart of Spark's [[org.apache.spark.sql.types.StructType]].
 *
 * @param fields A sequence of [[DType]], corresponding to the fields under [[Struct]]
 */
case class Struct(fields: Seq[DType], override val sqlType: StructType) extends BaseType {
  override lazy val itemSize: Int = fields.map(_.size).sum
  override protected def descr: String = s"[${fields.map(_.describe).mkString(", ")}]"
  override lazy val describe: String = descr

  private lazy val dataTypes: Seq[DataType] = sqlType.fields.map(_.dataType)

  // byte string has no byte order, as the data are bytes
  override val byteOrder: Option[ByteOrder] = None

  override def encode(buffer: ByteBuffer, value: Any): Unit = value match {
    case row: InternalRow =>
      fields.iterator.zipWithIndex.foreach { case (dtype, index) =>
        dtype.encode(buffer, row.get(index, dataTypes(index)))
      }
    case row: Row =>
      fields.iterator.zipWithIndex.foreach { case (dtype, index) =>
        dtype.encode(buffer, row.get(index))
      }
    case aSeq: Seq[_] =>
      fields.zip(aSeq).foreach { case (dtype, fieldValue) =>
        dtype.encode(buffer, fieldValue)
      }
    case invalid =>
      throw new IllegalArgumentException(s"Cannot encode $invalid as a Struct")
  }
}
