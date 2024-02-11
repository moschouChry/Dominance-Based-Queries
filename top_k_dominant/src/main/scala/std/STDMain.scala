package std

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import skyline.{AnglePartitioner, SkylineCalculator, SkylineCalculatorUtils}
import std.point.{Point, TopKResultPoint}

import scala.collection.convert.ImplicitConversions.`collection asJava`

object STDMain {
  private val distributions: List[String] = List("anticorrelated")
  private val coresList: List[Int] = List(16)
  private val kList: List[Int] = List(10)

  var dimensions: List[Int] = List()
  private var numSamplesList: List[Int] = List()

  if (sys.env("EXPERIMENT_TYPE") == "dimensions") {
    numSamplesList = List(1000000)
    dimensions = List(8)
  } else { // samples
    numSamplesList = List(100000, 1000000, 10000000)
    dimensions = List(6)
  }

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    distributions.forEach(distribution => {
      coresList.forEach(NUM_OF_CORES => {
        kList.forEach(K => {
          dimensions.forEach(dimension => {
              numSamplesList.forEach(numSamples => {
                var sum = 0.0
                val INPUT_FILE_PATH = s"data/${sys.env("EXPERIMENT_TYPE")}/${distribution}_data_${dimension}D_${numSamples}S.csv"

                println(s"###### ${distribution}_data_${dimension}D_${numSamples}S.csv K=$K CORES=$NUM_OF_CORES ######")

                1.to(1).foreach(iteration => {
                  val startTime = System.nanoTime()
                  println(s"local[$NUM_OF_CORES]")

                  val spark = SparkSession.builder()
                    .appName("Top-k dominating")
                    .master(s"local[$NUM_OF_CORES]")
                    .getOrCreate()

                  spark.sparkContext.setLogLevel("WARN")

                  import spark.implicits._
                  val dataset = spark
                    .read
                    .option("header", "false")
                    .option("inferSchema", "true")
                    .textFile(INPUT_FILE_PATH)
                    .map(row => Point(row.split(",").map(value => value.toDouble).toVector))
                    .as[Point]

                  val skylineUtils: SkylineCalculatorUtils = new SkylineCalculatorUtils(spark)

                  val partitioner = new AnglePartitioner(spark, dataset, NUM_OF_CORES)
                  val skylineCalculator: SkylineCalculator = new SkylineCalculator(spark, partitioner)

                  val std = new STD(spark, K)

                  val topK: Vector[TopKResultPoint] = std(dataset, skylineCalculator, skylineUtils)

                  val topKDataset: Dataset[TopKResultPoint] = spark.createDataset(topK).sort("k")
                  topKDataset.take(K).foreach(point => println(point))

                  val endTime = System.nanoTime()
                  val elapsedTime = (endTime - startTime) / 1e9 // Convert nanoseconds to seconds
                  println(s"Iteration: $iteration Elapsed time: $elapsedTime seconds")
                  sum += elapsedTime

                  spark.stop()
                })

                println(s"Mean execution time ${sum / 10.0}")
              })
            })
          })
      })
    })
  }
}
