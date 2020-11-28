package io.xydrolase.spark.npy

import java.nio.{ByteBuffer, ByteOrder => JByteOrder}

import org.apache.spark.sql.catalyst.util.ArrayData

/**
 * Corresponds to the Numpy data type (`dtype`) associated with a Numpy array.
 *
 * @param baseType
 * @param name The name of the numpy data field.
 * @param shape The shape of the data type. For scalar data types (e.g. float64), the shape is an empty sequence.
 *              When `shape` is a non-empty sequence, [[DType]] effectively represents an [[ArrayType]] column in
 *              Spark.
 */
case class DType(baseType: BaseType, name: Option[String] = None, shape: Seq[Int] = Seq.empty) {
  @transient private lazy val internalBuffer: ByteBuffer = baseType.byteOrder match {
    case None => ByteBuffer.allocate(size)
    case Some(Native) => ByteBuffer.allocate(size).order(JByteOrder.nativeOrder())
    case Some(LittleEndian) => ByteBuffer.allocate(size).order(JByteOrder.LITTLE_ENDIAN)
    case Some(BigEndian) => ByteBuffer.allocate(size).order(JByteOrder.BIG_ENDIAN)
  }

  /**
   * @return Number of dimension of the sub-array if this data type describes a sub-array
   */
  def ndim: Int = shape.length

  // number of elements after flatten the internal shape
  lazy val numOfElements: Int = shape.foldLeft(1) { case (dimSize, size) => size * dimSize }

  /**
   * @return The size of the data type, in terms of bytes, when stored in memory and in the numpy
   *         file.
   */
  def size: Int = baseType.itemSize * numOfElements

  /**
   * @return The description of the data-type that is largely compatible with Numpy dtype's `descr`
   *         method.
   */
  def describe: String = {
    (name, ndim) match {
      case (None, 0) => baseType.describe
      case (None, _) => s"(${baseType.describe}, $describeShape)"
      case (Some(typeName), 0) =>
        s"('$typeName', ${baseType.describe})"
      case (Some(typeName), _) =>
        s"('$typeName', ${baseType.describe}, $describeShape)"
    }
  }

  /**
   * Broadcast the base type into an nd-array with provided shape.
   * @param shape
   * @return
   */
  def broadcast(shape: Seq[Int]): DType = copy(shape = shape)

  def withName(name: String): DType = copy(name = Some(name))

  private def describeShape: String = {
    shape.toList match {
      case Nil => ""
      // Python 1-tuple syntax
      case dim :: Nil => s"($dim,)"
      case s => s"(${s.mkString(",")})"
    }
  }

  private def pad(buffer: ByteBuffer, numOfBytes: Int): Unit = {
    val zero: Byte = 0
    (0 until numOfBytes * baseType.itemSize).foreach { _ =>
      buffer.put(zero)
    }
  }

  def encode(buffer: ByteBuffer, value: Any): Unit = value match {
    case arrayData: ArrayData =>
      val elems = arrayData.numElements()
      internalBuffer.clear()
      (0 until elems).foreach { index =>
        baseType.encode(internalBuffer, arrayData.get(index, baseType.sqlType))
      }

      if (elems < numOfElements) {
        pad(internalBuffer, numOfElements - elems)
      }

      buffer.put(internalBuffer.array())
    case seq: Seq[_] =>
      require(shape.nonEmpty)

      if (seq.length > numOfElements) {
        throw new IndexOutOfBoundsException(
          s"Sequence has ${seq.length} elements, but the limit is $numOfElements"
        )
      }

      internalBuffer.clear()
      seq.foreach { v => baseType.encode(internalBuffer, v) }
      // pad the internal buffer to the maximum size
      if (seq.length < numOfElements) {
        pad(internalBuffer, numOfElements - seq.length)
      }

      buffer.put(internalBuffer.array())
    case arr: Array[_] =>
      encode(buffer, arr.toSeq)
    case _ =>
      baseType.encode(buffer, value)
  }
}

object DType {
  // some constant dtype's
  val bool: DType = DType(Bool)
  val float32: DType = DType(Float32)
  val float64: DType = DType(Float64)
  val int32: DType = DType(Int32)
  val int64: DType = DType(Int64)
}