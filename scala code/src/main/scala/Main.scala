import SkylineAlgorithms.{alsAlgorithm, sfsAlgorithm}
import org.apache.hadoop.security.UserGroupInformation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)
    // To avoid a specific error in run. It can be removed in the final deliverable
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("test"))

    // Define configuration variables
    val appName = "DWS Dominance Based Queries"
    val masterUrl = "local[16]" // Use local[*] to utilize all available cores on local machine

    // Configure Spark
    val sparkConf = new SparkConf()
      .setAppName(appName)
      .setMaster(masterUrl)
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    // Access SparkContext from SparkSession
    val sc = spark.sparkContext

    // Open Spark UI in the browser
    val uiUrl = sc.uiWebUrl.getOrElse("Spark UI URL not available")
    println(s"Spark UI available at: $uiUrl")

    // Define the dataset parameters
    val distribution = "normal"
    val dimensions = 4
    val samples = 10000000

    // Construct the file name based on the variables
    val fileName = s"data/${distribution}_data_${dimensions}D_${samples}S.csv"

    // Load dataset into an RDD
    val numPartitions = 16
    val data: RDD[Array[Double]] = sc.textFile(fileName, numPartitions)
      .map(line => line.split(",").map(_.toDouble))
    println(s"File $fileName successfully read into RDD.")

    val numPartitions_test = data.getNumPartitions
    println(s"Number of partitions in RDD: $numPartitions_test")


    // Use the distributed SFS algorithm
    val startTimeSFS = System.nanoTime()
    val skylineSFS = sfsAlgorithm(data)
    val endTimeSFS = System.nanoTime()
    val estimationTimeSFS =  endTimeSFS - startTimeSFS

    println("SFS Done")

    // Use the distributed ALS algorithm
    val startTimeALS = System.nanoTime()
    val skylineALS = alsAlgorithm(data)
    val endTimeALS = System.nanoTime()
    val estimationTimeALS =  endTimeALS - startTimeALS

    println("ALS Done")

    println("SFS implementation")
    println(s"Count: ${skylineSFS.length}, Time: ${estimationTimeSFS / 1e9} seconds")

    println("ALS implementation")
    println(s"Count: ${skylineALS.length}, Time: ${estimationTimeALS / 1e9} seconds")

    // To prevent the application from exiting immediately
    Thread.sleep(3600000) // Sleep for 1 hour (3600 seconds)

    spark.stop() // Stop the Spark session
  }

}
