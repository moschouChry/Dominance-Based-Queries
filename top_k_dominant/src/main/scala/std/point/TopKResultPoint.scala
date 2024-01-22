package std.point

case class TopKResultPoint(k: Int, point: Point, dominanceScore: Long) extends Serializable