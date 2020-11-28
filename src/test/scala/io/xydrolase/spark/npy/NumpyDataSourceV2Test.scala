package io.xydrolase.spark.npy

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SQLImplicits, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

object NumpyDataSourceV2Test {
  case class TrainingExample(timestamp: Long, label: Int, weight: Double, featureVector: Array[Double])
}

class NumpyDataSourceV2Test extends SQLImplicits with AnyWordSpecLike with Matchers with BeforeAndAfterAll {
  import NumpyDataSourceV2Test._

  private var _spark: SparkSession = _

  implicit def spark: SparkSession = _spark

  // bind the _sqlContext variable to the Spark session for all implicit converters
  override def _sqlContext: SQLContext = spark.sqlContext

  override def beforeAll(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)

    _spark = SparkSession.builder().config(sparkConf).getOrCreate()
  }

  override def afterAll(): Unit = {
    Option(_spark).foreach(_.stop())
  }

  "A NumpyDataSourceV2" should {
    "dump DataFrame in .npy file output" in {
      val df = Seq(
        TrainingExample(10000L, 1, 1.0, Array(3.0, 4.0, 5.0, 6.0)),
        TrainingExample(10052L, 0, 0.3, Array(1.0, 0.4, 3.2, 2.8)),
        TrainingExample(10059L, 0, 0.4, Array(0.0, 0.7, 9.2, 0.8)),
        TrainingExample(10073L, 0, 0.2, Array(1.0, 1.5, 2.4, 0.4))
      ).toDS.repartition(1).sortWithinPartitions($"timestamp")

      df.write
        .format("io.xydrolase.spark.npy.NumpyDataSourceV2")
        .mode(SaveMode.Overwrite)
        .option("maximumSize:featureVector", "4")
        .save("/tmp/test.npy")
    }
  }
}
