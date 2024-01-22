package std


import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}
import std.point.{Point, PointWithDominanceScore, TopKResultPoint}
import std.skyline.{SkylineCalculator, SkylineCalculatorUtils}

import scala.collection.mutable

class STD(ss: SparkSession, k: Int) extends Serializable {

  def apply(dataset: Dataset[Point], skylineCalculator: SkylineCalculator, skylineUtils: SkylineCalculatorUtils): Vector[TopKResultPoint] = {
    val skyline: Vector[Point] = skylineCalculator.BlockNestedLoop(dataset)

    val skylineWithScore: Vector[PointWithDominanceScore] = skylineUtils.calculateDominanceScore(skyline, dataset)

    val PQ = new mutable.PriorityQueue[PointWithDominanceScore]()
    PQ ++= skylineWithScore

    var currentDataset: Dataset[Point] = dataset

    1.to(k)
      .foldLeft(Vector[TopKResultPoint]())(
        (topKPoints: Vector[TopKResultPoint], k: Int) => {
          val curTopK: PointWithDominanceScore = PQ.dequeue()

          val curSkylineBV: Broadcast[Vector[PointWithDominanceScore]] = ss.sparkContext.broadcast(PQ.toVector)
          val curTopBV: Broadcast[PointWithDominanceScore] = ss.sparkContext.broadcast(curTopK)

          val exclusiveDomRegion: Dataset[Point] = currentDataset
            .filter(dp => curTopBV.value.point.dominates(dp))
            .filter(dp => !curSkylineBV.value.exists(sp => sp.point.dominates(dp)))

          if (exclusiveDomRegion.isEmpty) {
            topKPoints :+ TopKResultPoint(k, curTopK.point, curTopK.score)
          } else {
            val domRegionSkyline: Vector[Point] = skylineCalculator.BlockNestedLoop(exclusiveDomRegion)

            currentDataset = currentDataset.filter(p => !p.equals(curTopK.point))

            val domRegionSkylineWithDom: Vector[PointWithDominanceScore] = skylineUtils.calculateDominanceScore(domRegionSkyline, currentDataset)

            PQ ++= domRegionSkylineWithDom

            topKPoints :+ TopKResultPoint(k, curTopK.point, curTopK.score)
          }
        }
      )

  }

}