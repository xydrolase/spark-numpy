package io.xydrolase.spark

import java.nio.{ ByteOrder => JByteOrder }

package npy {
  sealed trait ByteOrder extends Serializable {
    def toJava: JByteOrder
    def describe: Char
  }

  case object LittleEndian extends ByteOrder {
    override val toJava: JByteOrder = JByteOrder.LITTLE_ENDIAN
    override val describe: Char = '<'
  }

  case object BigEndian extends ByteOrder {
    override val toJava: JByteOrder = JByteOrder.BIG_ENDIAN
    override val describe: Char = '>'
  }

  case object Native extends ByteOrder {
    override val toJava: JByteOrder = JByteOrder.nativeOrder()
    override val describe: Char = toJava match {
      case JByteOrder.BIG_ENDIAN => '>'
      case _ => '<'
    }
  }
}
