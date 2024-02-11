package std.skyline

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import std.point.{PartitionedPoint, Point}

class SkylineCalculator(spark: SparkSession, partitioner: AnglePartitioner) extends Serializable {

  import spark.implicits._

  def BlockNestedLoop(dataset: Dataset[Point]): Vector[Point] = {

    def myCombiner(ppi: Iterator[PartitionedPoint]): Iterator[Vector[Point]] = {
      val localSkyline: Vector[Point] = ppi.map(_.point).toVector

      // Remove the dominated points
      val dominatedPoints: Set[Point] = localSkyline.foldLeft(Set[Point]()) {
        (dominated: Set[Point], candidate: Point) => {
          val pointsToRemove: Set[Point] =
            if (dominated.contains(candidate)) {
              Set[Point]()
            } else {
              val dominatedByCandidate: Set[Point] = localSkyline
                .filter(p => candidate.dominates(p))
                .toSet[Point]

              if (localSkyline.exists(_.dominates(candidate))) {
                dominatedByCandidate + candidate
              } else {
                dominatedByCandidate
              }
            }
          dominated.union(pointsToRemove)
        }
      }

      val result: Vector[Point] = localSkyline.diff(dominatedPoints.toVector)
      Iterator[Vector[Point]](result)
    }

    def myReducer(firstSkyline: Vector[Point], secondSkyline: Vector[Point]): Vector[Point] = {
      if (firstSkyline == null && secondSkyline == null) {
        return Vector[Point]()
      }
      if (firstSkyline == null) {
        return secondSkyline
      }
      if (secondSkyline == null) {
        return firstSkyline
      }
      if (firstSkyline.isEmpty) {
        return secondSkyline
      }
      if (secondSkyline.isEmpty) {
        return firstSkyline
      }

      val secondSkylineFinal: Vector[Point] = firstSkyline.foldLeft(secondSkyline) {
        (acc: Vector[Point], v1_point: Point) => {
          // Keep all points that are not dominated by first skyline
          acc.filter(v2_point => !v1_point.dominates(v2_point))
        }
      }

      val firstSkylineFinal: Vector[Point] = secondSkyline.foldLeft(firstSkyline) {
        (acc: Vector[Point], v2_point: Point) => {
          // Keep all points that are not dominated by second skyline
          acc.filter(v1_point => !v2_point.dominates(v1_point))
        }
      }

      firstSkylineFinal ++ secondSkylineFinal
    }

    val partitionedDataset: Dataset[PartitionedPoint] = dataset
      .map(p => PartitionedPoint(p, partitioner.partition(p)))
      .as[PartitionedPoint]

    val localSkylines: Dataset[Vector[Point]] = partitionedDataset
      .repartitionByRange(col("partition"))
      .mapPartitions((ppi: Iterator[PartitionedPoint]) => myCombiner(ppi))

    val globalSkyline: Vector[Point] = localSkylines
      .reduce((a: Vector[Point], b: Vector[Point]) => myReducer(a, b))

    globalSkyline
  }
}