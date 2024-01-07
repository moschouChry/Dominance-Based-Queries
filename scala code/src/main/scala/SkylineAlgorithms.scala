import org.apache.spark.rdd.RDD

object SkylineAlgorithms {

  def isDominated(point: Array[Double], otherPoint: Array[Double]): Boolean = {
    var allEqualOrWorse = true
    var atLeastOneWorse = false

    for (i <- point.indices) {
      if (point(i) < otherPoint(i)) allEqualOrWorse = false
      if (point(i) > otherPoint(i)) atLeastOneWorse = true
    }

    allEqualOrWorse && atLeastOneWorse
  }

  def computeLocalSkyline(partitionData: Iterator[Array[Double]]): Iterator[Array[Double]] = {
    var skyline = List[Array[Double]]()

    partitionData.foreach { point =>
      skyline = skyline.filter(notDominatedPoint => !isDominated(notDominatedPoint, point))
      if (!skyline.exists(p => isDominated(point, p))) {
        skyline = point :: skyline
      }
    }

    skyline.iterator
  }

  def alsAlgorithm(data: RDD[Array[Double]]): Array[Array[Double]] = {

    // Step 1: Compute local skylines in each partition
    val localSkylines = data.mapPartitions(computeLocalSkyline)

    // Step 2: Collect all local skylines to the driver
    val allLocalSkylinePoints = localSkylines.collect()

    // Step 3:  Compute the global skyline
    val finalSkyline = allLocalSkylinePoints.filter { point =>
      !allLocalSkylinePoints.exists(otherPoint => isDominated(point, otherPoint))
    }

    finalSkyline
    //data.context.parallelize(finalSkyline)
  }

  def monotonicScoringFunction(point: Array[Double]): Double = {
    // Example: Sum of all dimensions. Modify as needed.
    point.sum
  }

  def sfsAlgorithm(data: RDD[Array[Double]]): List[Array[Double]] = {

    // Step 1: Sort the data based on the scoring function
    val sortedData = data.sortBy(monotonicScoringFunction)
    // Step 2: Apply a filter to find the skyline points
    val finalSkyline = sortedData.collect().foldLeft(List[Array[Double]]()) { (finalSkyline, point) =>
      if (!finalSkyline.exists(p => isDominated(point, p))) {
        point :: finalSkyline
      } else {
        finalSkyline
      }
    }

    finalSkyline
  }

}
