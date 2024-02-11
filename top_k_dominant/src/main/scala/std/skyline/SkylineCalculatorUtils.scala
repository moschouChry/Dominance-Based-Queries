package std.skyline

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}
import std.point.{Point, PointWithDominanceScore}

class SkylineCalculatorUtils(ss: SparkSession) extends Serializable {

  import ss.implicits._

  def calculateDominanceScore(skyline: Vector[Point], dataset: Dataset[Point]): Vector[PointWithDominanceScore] = {

    def updateScore(pointWithDom: PointWithDominanceScore, inputPoint: Point): PointWithDominanceScore = {
      if (pointWithDom.point.dominates(inputPoint)) {
        PointWithDominanceScore(pointWithDom.point, pointWithDom.score + 1)
      } else {
        pointWithDom
      }
    }

    def mapperFunc(skylineBV: Broadcast[Vector[Point]])(partitionPoints: Iterator[Point]): Iterator[Vector[PointWithDominanceScore]] = {
      val localSkyline: Vector[Point] = skylineBV.value
      val localSkylineWithScores: Vector[PointWithDominanceScore] = localSkyline.map(p => PointWithDominanceScore(p, 0L))
      partitionPoints.map(p => localSkylineWithScores.map(pwd => updateScore(pwd, p)))
    }

    val skylineBV: Broadcast[Vector[Point]] = ss.sparkContext.broadcast(skyline)

    val localSkylinesWithDom: Dataset[Vector[PointWithDominanceScore]] = dataset.mapPartitions(mapperFunc(skylineBV))

    localSkylinesWithDom.reduce((s1: Vector[PointWithDominanceScore], s2: Vector[PointWithDominanceScore]) => {
      s1.zip(s2).map(pair => PointWithDominanceScore(pair._1.point, pair._1.score + pair._2.score))
    })
  }
}