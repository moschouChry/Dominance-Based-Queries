import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object Main {
  def main(args: Array[String]): Unit = {
    val NUM_OF_CORES = sys.env.get("NUM_OF_CORES")
    val INPUT_FILE_PATH = sys.env.get("INPUT_FILE_PATH")
    val K = sys.env.get("K")
    println(s"local[${NUM_OF_CORES.get}]")
    val spark = SparkSession.builder()
      .appName("Top-k dominating")
      .master(s"local[${NUM_OF_CORES.get}]")
      .config("spark.executor.cores", NUM_OF_CORES.get.toInt)
      .config("spark.executor.instances", NUM_OF_CORES.get.toInt)
      .getOrCreate()

    val hashMapAccumulator = spark.sparkContext.collectionAccumulator[Map[String, Int]]("HashMapAccumulator")

    val startTime = System.nanoTime()
    val inputFileRDD = spark
      .sparkContext
      .textFile(INPUT_FILE_PATH.get, NUM_OF_CORES.get.toInt)

    val inputFileRDDWithId = inputFileRDD
      .zipWithIndex()
      .map { case (line, index) => s"$index,$line" }

    val inputFileRDDStringRows = inputFileRDDWithId
      .map(_.split(","))
      .map(attributes => Row.fromSeq(attributes))

    def convertRowOfStringsToRowOfDoubles(row: Row): Row = {
      val floatValues = row.toSeq.map {
        case stringValue: String => stringValue.toDouble
        case _ => 0.0.toDouble
      }
      Row.fromSeq(floatValues)
    }

    val inputFileRDDDoubleRows = inputFileRDDStringRows.map(convertRowOfStringsToRowOfDoubles)

    // 4.1 Data Map and Ranking phase
    def splitRowsToIDAndSingleAttribute(row: Row): Seq[Row] = {
      val id = row.getDouble(0)
      val otherColValues = row.toSeq.asInstanceOf[Seq[Double]].tail.zipWithIndex

      // Generate all possible combinations of OID and attribute values
      val combinations = otherColValues.map{ case (value, index) => Row(id.toInt, value, index) }

      combinations
    }

    val inputFileRDDSingleAttribute = inputFileRDDDoubleRows.flatMap(splitRowsToIDAndSingleAttribute)
    val inputFileRDDSingleAttributeGrouped = inputFileRDDSingleAttribute.groupBy(row => row.get(2))

    val firstPhaseRDD = inputFileRDDSingleAttributeGrouped.flatMap{row => {
      var sortedIterable = row._2.toList.sortBy(row => row(1).asInstanceOf[Double])

      sortedIterable = sortedIterable.zipWithIndex.map{ case (nestedRow, index) => Row(nestedRow(0), index + 1, nestedRow(2))}
      println
      sortedIterable
    }}

    // 4.2 Shuffling - 4.3 Worst Rank Computation
    // firstPhaseRDD schema: OID, Rank, attribute index
    val worstRankRDD = firstPhaseRDD
      .groupBy(row => row.get(0))
      .map{row =>{
        var worstRank = -1
        var worstRankAttributeIndex = -1

        row._2.foreach(nestedRow => {
          if (nestedRow(1).toString.toInt > worstRank) {
            worstRank = nestedRow(1).toString.toInt
            worstRankAttributeIndex = nestedRow(2).toString.toInt
          }
        })

        Row(row._1.toString.toInt, worstRankAttributeIndex, worstRank)
      }}

    // 4.4 DC Sets Computation
    // worstRankRDD schema: OID, worst rank attribute index, worst rank value
    // firstPhaseRDD schema: OID, Rank, attribute index
    // NOTE: DataFrame API has theta join with <
    val worstRankPairedRDD: RDD[(String, Array[String])] = worstRankRDD
      .map(row => (row(1).toString, row.toSeq.map(value => value.toString).toArray))

    val firstPhasePairedRDD: RDD[(String, Array[String])] = firstPhaseRDD
      .map(row => (row(2).toString, row.toSeq.map(value => value.toString).toArray))

    val joinedRDD = worstRankPairedRDD
      .join(firstPhasePairedRDD)

    val dcRDD = joinedRDD
      .filter{ case (key, value) => value._1(2).toInt < value._2(1).toInt }
      .map{case (key, value) => (value._1(0), value._2(0))}

    // 4.5 Skyband and Dominating Objects Computation
    // dcRDD schema: OID1, OID2 for object pairs to check dominance
    val inputFilePairedRDD = inputFileRDDWithId
      .map(row => (row.split(',')(0), row.split(',').tail))

    val joinedRDDWithObjectVectors = dcRDD
      .join(inputFilePairedRDD)
      .map{row => {
        (row._2._1, (row._1, row._2._2))
      }}
      .join(inputFilePairedRDD)
      .map{row => {
        Row(row._1, row._2._2, row._2._1._1, row._2._1._2)
      }}

//    TODO see how many partitions joinedRDDWithObjectVectors has and set them equal to the number of executors
    // the second object should be checked if it dominates the first
//    val joinedRDDWithObjectVectorsRepartioned = joinedRDDWithObjectVectors.repartition(NUM_OF_CORES.get.toInt)

    println(joinedRDDWithObjectVectors.getNumPartitions)

    joinedRDDWithObjectVectors.foreach(row => {
      val object2 = row(3)
        .asInstanceOf[Array[String]]
        .map(value => value.toDouble)

      val object1 = row(1)
        .asInstanceOf[Array[String]]
        .map(value => value.toDouble)

      var object2DominatesObject1 = true

      object2.zip(object1).foreach{case (object2value, object1value) =>
        if (object2value > object1value) {
          object2DominatesObject1 = false
        }
      }

      if (object2DominatesObject1) {
        val updatedMap = Map(s"${row(2)}" -> 1)

        hashMapAccumulator.add(updatedMap)
      }
    })

    // NOTE we can keep a data structure (like a priority queue) of K positions and iterate over the values one time instead of sorting
    val finalHashMap = hashMapAccumulator.value.flatten.groupBy(_._1).mapValues(_.map(_._2).sum)
    val topKeys = finalHashMap.toList.sortBy(-_._2).take(K.get.toInt).map(_._1)
    val endTime = System.nanoTime()
    val elapsedTime = (endTime - startTime) / 1e9 // Convert nanoseconds to seconds

    println(s"Keys with the highest $K values: $topKeys")
    println(s"Elapsed time: $elapsedTime seconds")
  }
}