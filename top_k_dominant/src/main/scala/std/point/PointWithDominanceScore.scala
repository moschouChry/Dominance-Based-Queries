package std.point

case class PointWithDominanceScore(point: Point, score: Long) extends Serializable with Comparable[PointWithDominanceScore] {

  override def compareTo(other: PointWithDominanceScore): Int = (this.score - other.score).signum

}