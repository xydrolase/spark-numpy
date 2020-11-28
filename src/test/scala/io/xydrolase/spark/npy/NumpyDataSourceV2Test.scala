package io.xydrolase.spark.npy

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SQLImplicits, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

object NumpyDataSourceV2Test {
  case class Features(botScore: Float, dwellTime: Float, eventsInLastHour: Float)
  case class TrainingExample(timestamp: Long, label: Int, weight: Double, featureVector: Array[Double])
  case class TrainingExampleNested(timestamp: Long, labels: Seq[Int], weight: Double, features: Seq[Features])
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

    "dump DataFrame with nested schema in .npy file output" in {
      val df = Seq(
        TrainingExampleNested(10000L, Seq(1, 0, 0, 0), 1.0,
          Seq(
            Features(0.92f, 0.33f, 433.2f),
            Features(0.12f, 5.2f, 11.2f),
            Features(0.52f, 7.9f, 9.2f),
            Features(0.32f, 19.9f, 3.2f)
          )
        ),
        TrainingExampleNested(10000L, Seq(0, 0, 0, 1), 1.0,
          Seq(
            Features(0.02f, 4.33f, 23.8f),
            Features(0.09f, 6.1f, 9.7f),
            Features(0.37f, 8.9f, 3.6f),
            Features(0.84f, 1.4f, 96.5f)
          )
        )
      ).toDS.repartition(1).sortWithinPartitions($"timestamp")

      df.write
        .format("io.xydrolase.spark.npy.NumpyDataSourceV2")
        .mode(SaveMode.Overwrite)
        .option("maximumSize:labels", "4")
        .option("maximumSize:features", "4")
        .save("/tmp/testNested.npy")
    }
  }
}
