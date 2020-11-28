package io.xydrolase.spark.npy

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.internal.Logging

import scala.util.Try

object NumpyOptions {
  val defaultArraySize = 16
  val defaultStringSize = 16
}

class NumpyOptions(@transient val parameters: CaseInsensitiveMap[String]) extends Logging with Serializable {
  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  def maxStringSize: Option[Int] = {
    parameters.get("maxStringSize").flatMap(s => Try(s.toInt).toOption)
  }

  def maxArraySize: Option[Int] = {
    parameters.get("maxArraySize").flatMap(s => Try(s.toInt).toOption)
  }

  def defaultArraySize: Int = {
    parameters.get("defaultArraySize").flatMap(s => Try(s.toInt).toOption).getOrElse(NumpyOptions.defaultArraySize)
  }

  def defaultStringSize: Int = {
    parameters.get("defaultStringSize").flatMap(s => Try(s.toInt).toOption).getOrElse(NumpyOptions.defaultArraySize)
  }

  def maximumSizeOf(path: Seq[String]): Option[Int] = {
    parameters.get(s"maximumSize:${path.mkString(".")}").flatMap(s => Try(s.toInt).toOption)
  }
}
