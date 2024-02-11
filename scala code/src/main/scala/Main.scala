import SkylineAlgorithms.{ALS_SFS_algorithm, ALS_algorithm, countSkylinePoints, determinePartition, partitionData}
import org.apache.commons.math3.geometry.Point
import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object Main {

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)
    // To avoid a specific error in run. It can be removed in the final deliverable
    // UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("test"))

    val available_cores = List(2, 4, 8, 16)

    for (cores <- available_cores) {
      // Define configuration variables
      // val cores = 16
      val appName = "DWS Dominance Based Queries"
      val masterUrl = "local[" + cores + "]" // Use local[*] to utilize all available cores on local machine

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


      // Prepare a mutable collection to gather your statistics
      val resultsList = ListBuffer[Array[Any]]()

      // Define the dataset parameters
      //val distribution = "anticorrelated"
      //val dimensions = 6
      //val samples = 10000000
      val available_distributions = List("normal", "uniform", "correlated", "anticorrelated")
      val available_dimensions = List(6)
      val available_samples = List(100000, 1000000, 10000000)

      for (distribution <- available_distributions) {
        for (dimensions <- available_dimensions) {
          for (samples <- available_samples) {

            // Construct the file name based on the variables

            val fileName = s"${distribution}_data_${dimensions}D_${samples}S"
            val complete_filePath = s"data/" + fileName + ".csv"

            // Prepare a mutable collection to gather your statistics
            val resultsList = ListBuffer[Array[Any]]()

            // Load dataset into an RDD
            val numPartitions = 32
            val data: RDD[Array[Double]] = sc.textFile(complete_filePath, numPartitions)
              .map(line => line.split(",").map(_.toDouble))
            println(s"File $fileName successfully read into RDD.")

            println(s"Number of partitions in RDD: ${data.getNumPartitions}, Number of cores: $cores")

            //////////////////////////////////////////////////////////////

            //////////////////////////////////////////////////////////////

            println("ALS implementation")
            val startTimeALS = System.nanoTime()
            val skylineALS = ALS_algorithm(data)
            val countSkylineALS = countSkylinePoints(skylineALS, fileName, storePoints = true)
            val endTimeALS = System.nanoTime()
            val estimationTimeALS = endTimeALS - startTimeALS
            println(s"Count: ${countSkylineALS}, Time: ${estimationTimeALS / 1e9} seconds")
            resultsList += Array("ALS", distribution, samples, dimensions, cores, estimationTimeALS / 1e9,countSkylineALS)
            /////////////////////////////////////////////////////////////////

            println("ALS with SFS implementation")
            val startTimeALSSFS = System.nanoTime()
            val skylineALSSFS = ALS_SFS_algorithm(data)
            val countSkylineALSSFS = countSkylinePoints(skylineALSSFS, fileName)
            val endTimeALSSFS = System.nanoTime()
            val estimationTimeALSSFS = endTimeALSSFS - startTimeALSSFS
            println(s"Count: ${countSkylineALSSFS}, Time: ${estimationTimeALSSFS / 1e9} seconds")
            resultsList += Array("ALS_SFS", distribution, samples, dimensions, cores, estimationTimeALSSFS / 1e9,countSkylineALSSFS)
            /////////////////////////////////////////////////////////////////


            // Path to your CSV file
            val csvFilePath = "results/Skyline_Time_vs_Samples.csv"

            // Check if the file exists
            val file = new File(csvFilePath)
            val fileExists = file.exists()

            // Function to write data to a CSV file
            def writeToCsv(data: ListBuffer[Array[Any]], append: Boolean = false): Unit = {
              val fileWriter = new FileWriter(csvFilePath, append)
              val bufferedWriter = new BufferedWriter(fileWriter)
              val printWriter = new PrintWriter(bufferedWriter)

              data.foreach { row =>
                printWriter.println(row.mkString(","))
              }

              // Close all the writers
              printWriter.close()
              bufferedWriter.close()
              fileWriter.close()
            }


            // Append to CSV if it exists, otherwise create new
            writeToCsv(resultsList, fileExists)
          }
        }
      }


      println("Done!")


      // To prevent the application from exiting immediately
      // Thread.sleep(3600000) // Sleep for 1 hour (3600 seconds)

      spark.stop() // Stop the Spark session
    }

  }

}
