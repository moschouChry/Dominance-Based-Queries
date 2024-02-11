import scala.math._
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.{Files, Paths}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

object SkylineAlgorithms {

  def dominates(otherPoint: Array[Double], point: Array[Double]): Boolean = {

    // All coordinates of point are equal or worse than the corresponding coordinates of otherPoint
    var allEqualOrWorse = true
    // At least one coordinate of point is worse than the corresponding coordinate of otherPoint
    var atLeastOneWorse = false

    for (i <- point.indices) {
      if (point(i) < otherPoint(i)) allEqualOrWorse = false
      if (point(i) > otherPoint(i)) atLeastOneWorse = true
    }

    allEqualOrWorse && atLeastOneWorse
  }

  def ALS_algorithm(data: RDD[Array[Double]]): Iterator[Array[Double]] = {

    // Step 1: Compute local skylines in each partition
    val localSkylines = data.mapPartitions(simpleSkylineComputation)

    // Step 2: Collect all local skylines to the driver
    val allLocalSkylinePoints = localSkylines.collect()

    // Step 3: Compute the final skyline
    val finalSkylineSet = simpleSkylineComputation(allLocalSkylinePoints.iterator)

    finalSkylineSet
  }

  def ALS_SFS_algorithm(data: RDD[Array[Double]]): Iterator[Array[Double]] = {

    // Step 1: Compute local skylines in each partition
    val localSkylines = data.mapPartitions(SFSskylineCalculation)

    // Step 2: Collect all local skylines to the driver
    val allLocalSkylinePoints = localSkylines.collect()

    // Step 3: Compute the final skyline
    val finalSkylineSet = SFSskylineCalculation(allLocalSkylinePoints.iterator)

    finalSkylineSet
  }

  def simpleSkylineComputation(inputData: Iterator[Array[Double]]): Iterator[Array[Double]] = {

    // Step 1: Initialize an empty window
    val skylineSet = ArrayBuffer[Array[Double]]()

    // Step 2: Convert the RDD into an array for processing
    val data = inputData.toArray

    // Step 3: Handle the case where the data is empty
    if (data.isEmpty){return skylineSet.iterator}

    // Step 4: Add the first element to the window
    skylineSet += data(0)

    // Step 5: Iterate through the remaining elements
    for (i <- 1 until data.length) {
      var j = 0
      var discard = false

      val loop = new Breaks
      loop.breakable {
        while (j < skylineSet.length) {
          if ( dominates(data(i), skylineSet(j)) ) {
            skylineSet.remove(j)
            j -= 1
          }
          else if (dominates(skylineSet(j), data(i))) {
            discard = true
            loop.break
          }
          j += 1
        }
      }
      if (!discard)
        skylineSet.append(data(i))
    }
    skylineSet.iterator
  }

  def SFSskylineCalculation(partitionData: Iterator[Array[Double]]): Iterator[Array[Double]] = {
    // Convert iterator to list for multiple passes
    val scoredData = monotonicScoringFunction(partitionData).toArray
    val sortedData = scoredData.sortBy(_._2).iterator

    val skyline = simpleSkylineComputation(sortedData.map(_._1))

    skyline
  }

  def monotonicScoringFunction(data: Iterator[Array[Double]]): Iterator[(Array[Double], Double)] = {
    data.map(p => (p, p.sum))
  }


  def cartesianToHyperspherical(point: Array[Double]): Array[Double] = {
    val r = sqrt(point.map(x => x * x).sum)
    val angles = new Array[Double](point.length - 1)

    for (i <- 0 until point.length - 1) {
      val partialSum = sqrt(point.drop(i).map(x => x * x).sum)
      angles(i) = acos(point(i) / partialSum)
    }

    r +: angles // Return the radius followed by the angles
  }

  // Function to determine the partition
  def determinePartition(point: Array[Double], numPartitions: Int, dimensions: Int): Int = {
    // Composite score is the sum of all dimension values
    val compositeScore = point.sum

    // The range of the composite score is from 0 to the number of dimensions
    val compositeScoreRange = dimensions.toDouble

    // Calculate the partition size
    val partitionSize = compositeScoreRange / numPartitions

    // Determine the partition
    (compositeScore / partitionSize).toInt
  }

  def partitionData(rdd: RDD[Array[Double]], numPartitions: Int): RDD[(Int, Array[Double])] = {
    val hypersphericalRDD = rdd.map(cartesianToHyperspherical)

    // Assign points to partitions based on angular coordinates
    hypersphericalRDD.map(point => {
      val partition = determinePartition(point, numPartitions,6)
      (partition, point)
    })
  }


  // -------------------------------------------------------------------------------------------------------
  def countSkylinePoints(finalSkyline: Iterator[Array[Double]], fileName: String,
                    printPoints: Boolean = false,
                    storePoints: Boolean = false): Int = {

    // Convert the iterator to an array to enable multiple iterations
    val skylinePoints = finalSkyline.toArray

    // Count of skyline points
    val count = skylinePoints.length

    // Optionally print the skyline points
    if (printPoints) {
      println("Skyline Points:")
      skylinePoints.foreach(point => println(point.mkString(", ")))
    }

    // Optionally store the skyline points in a CSV file
    if (storePoints) {

      val filePath = s"data/" + fileName + "_skyline.csv"

      // Check if the file exists
      if (Files.exists(Paths.get(filePath))) {
        // File exists, skip storing
        println(s"Skyline file for $filePath already exists. Skipping storing.")
      } else {

        val writer = new BufferedWriter(new FileWriter("data/" + fileName + "_skyline.csv"))
        try {
          skylinePoints.foreach { point =>
            writer.write(point.mkString(", "))
            writer.newLine()
          }
        } finally {
          writer.close()
        }
      }

    }

    // Return the count of skyline points
    count
  }


}
