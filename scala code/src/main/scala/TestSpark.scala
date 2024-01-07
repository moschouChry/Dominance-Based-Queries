import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.{Level, Logger}


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object TestSpark {

  def main(args: Array[String]): Unit = {
    // Set the log level to WARN for Spark
    // Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getRootLogger.setLevel(Level.WARN)
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("test"))

    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("SparkExample")
      .master("local[16]")
      .getOrCreate()

    // Your Spark code here

    // Get the Spark UI URL
    val sparkUIUrl = spark.sparkContext.uiWebUrl.getOrElse("Spark UI URL not available")

    // Print the URL to the console
    println(s"Spark UI URL: $sparkUIUrl")

    // To keep the browser open, add a sleep or a user interaction
    // For example, you can use `Thread.sleep` to pause the execution
    Thread.sleep(60000) // Sleep for 60 seconds
  }

}
