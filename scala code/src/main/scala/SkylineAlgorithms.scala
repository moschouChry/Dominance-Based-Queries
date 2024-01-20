import org.apache.spark.rdd.RDD

import java.io.{BufferedWriter, FileWriter}
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


  // -------------------------------------------------------------------------------------------------------
  def countSkylinePoints(finalSkyline: Iterator[Array[Double]],
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
      val writer = new BufferedWriter(new FileWriter("skyline_points.csv"))
      try {
        skylinePoints.foreach { point =>
          writer.write(point.mkString(", "))
          writer.newLine()
        }
      } finally {
        writer.close()
      }
    }

    // Return the count of skyline points
    count
  }


}
