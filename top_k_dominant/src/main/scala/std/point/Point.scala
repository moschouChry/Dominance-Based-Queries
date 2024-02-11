package std.point

case class Point(dimensions: Vector[Double]) extends Serializable {

  def dim(i: Int): Double = dimensions.toList(i)

  def size: Int = dimensions.length

  def dominates(other: Point): Boolean = {
    if (this.equals(other)) {
      false
    } else {
      this.dimensions
        .zip(other.dimensions)
        .foldLeft(true)(
          (dominates: Boolean, d: (Double, Double)) => {
            d._1.compare(d._2).signum match {
              case -1 => dominates
              case 0 => dominates
              case 1 => false
            }
          }
        )
    }
  }

  //Equals
  def equals(other: Point): Boolean = this.dimensions == other.dimensions
}