import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.functions.{col, desc, sum, udf, when}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.broadcast

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable
import scala.util.Random
//import org.apache.spark.util.ShowL


//class CustomPartitioner[V](numPartitions1: Int) extends Partitioner {
////  private val SEED = 42
//  override def numPartitions: Int = numPartitions1
//
////  private val random = new Random(SEED)
//  def getPartition(key: Any): Int = {
//    // Implement your custom logic to assign a partition based on the key
//    // Example: Hash-based partitioning
//    key.toString.toInt % numPartitions
//  }
//
//
//}


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
      .config("spark.executor.memory", "3g")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.adaptive.skewJoin.enabled", "true")
//      .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256K")
      .config("spark.sql.shuffle.partitions", NUM_OF_CORES.get.toInt)
      .getOrCreate()

    val hashMapAccumulator = spark.sparkContext.collectionAccumulator[Map[String, Int]]("HashMapAccumulator")

    val startTime = System.nanoTime()
    val inputFileRDD = spark
      .sparkContext
      .textFile(INPUT_FILE_PATH.get, NUM_OF_CORES.get.toInt)

    val inputFileRDDWithId = inputFileRDD
      .zipWithIndex()
      .map { case (line, index) => s"${index.toInt},$line" }

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
      val combinations = otherColValues.map { case (value, index) => Row(id.toInt, value, index, Row.fromSeq(row.toSeq.tail)) }

      combinations
    }

    val inputFileRDDSingleAttribute = inputFileRDDDoubleRows.flatMap(splitRowsToIDAndSingleAttribute)
    val inputFileRDDSingleAttributeGrouped = inputFileRDDSingleAttribute.groupBy(row => row.get(2))

    val firstPhaseRDD = inputFileRDDSingleAttributeGrouped.flatMap { row => {
        var sortedIterable = row._2.toList.sortBy(row => row(1).asInstanceOf[Double])

        sortedIterable = sortedIterable.zipWithIndex.map { case (nestedRow, index) => Row(nestedRow(0), index + 1, nestedRow(2), nestedRow(3)) }
//        println
        sortedIterable
      }
    }

    // 4.2 Shuffling - 4.3 Worst Rank Computation
    // firstPhaseRDD schema: OID, Rank, attribute index, object values
    val worstRankRDD = firstPhaseRDD
      .groupBy(row => row.get(0))
      .map { row => {
        var worstRank = -1
        var worstRankAttributeIndex = -1
        var objectVector: Any = List()

        row._2.foreach(nestedRow => {
          if (nestedRow(1).toString.toInt > worstRank) {
            worstRank = nestedRow(1).toString.toInt
            worstRankAttributeIndex = nestedRow(2).toString.toInt
            objectVector = nestedRow(3)
          }
        })

        Row(row._1.toString.toInt, worstRankAttributeIndex, worstRank, objectVector)
      }
      }

    // 4.4 DC Sets Computation
    // worstRankRDD schema: OID, worst rank attribute index, worst rank value, object values
    // firstPhaseRDD schema: OID, Rank, attribute index, object values
    // NOTE: DataFrame API has theta join with <
    val worstRankSchema = StructType(
      Seq(
        StructField("objectIdWorstRank", StringType, nullable = false),
        StructField("attributeIndex", StringType, nullable = false),
        StructField("worstRankValue", StringType, nullable = false),
        StructField("objectValuesWorstRank", StringType, nullable = false),
      )
    )

    val firstPhaseSchema = StructType(
      Seq(
        StructField("objectIdFirstPhase", StringType, nullable = false),
        StructField("rank", StringType, nullable = false),
        StructField("attributeIndex", StringType, nullable = false),
        StructField("objectValuesFirstPhase", StringType, nullable = false),
      )
    )

    val toDoubleArray = udf((str: String) => str
      .filter(_ != '[')
      .filter(_ != ']')
      .split(",")
      .map(_.toDouble)
    )

    val customCondition = udf((object1: Seq[Double], object2: Seq[Double]) => {
      object1.zip(object2).forall { case (value1, value2) => value1 < value2 }
    })

//    spark.createDataFrame(firstPhaseRDD, schema)
    val worstRankPairedDF = spark.createDataFrame(worstRankRDD.map(row => Row(row(0).toString, row(1).toString, row(2).toString, row(3).toString)), worstRankSchema)
//    worstRankPairedDF.count()
    val firstPhasePairedDF = spark.createDataFrame(firstPhaseRDD.map(row => Row(row(0).toString, row(1).toString, row(2).toString, row(3).toString)), firstPhaseSchema)
    worstRankPairedDF.persist(StorageLevel.MEMORY_AND_DISK)
    firstPhasePairedDF.persist(StorageLevel.MEMORY_AND_DISK)

//    val bcDf = spark.sparkContext.broadcast(firstPhasePairedDF)
    val thetaJoinCondition = col("worstRankPairedDF.attributeIndex") === col("firstPhasePairedDF.attributeIndex") && col("worstRankPairedDF.worstRankValue").cast(IntegerType) < col("firstPhasePairedDF.rank").cast(IntegerType)
//    joinedRDD.for
//    spark.sql.shu
    val topKDominant = worstRankPairedDF.alias("worstRankPairedDF")
      .join(broadcast(firstPhasePairedDF).alias("firstPhasePairedDF"), thetaJoinCondition, "inner")
//      .repartition(16, col("objectIdWorstRank"))
      //.repartition(col("objectIdWorstRank")) // TODO remove it
//      .withColumn("objectValuesWorstRankArray", toDoubleArray(col("objectValuesWorstRank")))
//      .withColumn("objectValuesFirstPhaseArray", toDoubleArray(col("objectValuesFirstPhase")))
//      .select("objectIdWorstRank", "objectValuesWorstRankArray", "objectIdFirstPhase", "objectValuesFirstPhaseArray")
//      .withColumn("shouldCount", customCondition(col("objectValuesWorstRankArray"), col("objectValuesFirstPhaseArray")))
//      .groupBy("objectIdWorstRank")
//      .agg(sum(when(col("shouldCount"), 1).otherwise(0)).as("dominanceScore"))
//      .orderBy(desc("dominanceScore"))
//      .limit(K.get.toInt)
//
//    topKDominant.foreach(row => {
//      println(row(1))
//    })
//      .select("objectIdWorstRank")
//      .limit(K.get.toInt)
//      .to

//    topKDominant.rdd.saveAsTextFile("./test.csv")
//      .agg(sum(when(customCondition(col("objectValuesWorstRankArray"), col("objectValuesFirstPhaseArray")) === true).otherwise(0)).as("dominanceScore"))
//      .groupBy("worstRankPairedDF.objectIdWorstRank")
//      .map(row => (row._1 + "_" + Random.nextInt(NUM_OF_CORES.get.toInt).toString, row._2))
//      .partitionBy(new HashPartitioner(NUM_OF_CORES.get.toInt))
//      .groupByKey(NUM_OF_CORES.get.toInt)
//      .repartition(NUM_OF_CORES.get.toInt)
//    joinedRDD.foreach(row => {
//      val object2 = row(7)
//              .toString.filter(_ != '[')
//              .filter(_ != ']')
//              .split(',')
//              .map(value => value.toString.toDouble)
//
//            val object1 = row(3)
//              .toString.filter(_ != '[')
//              .filter(_ != ']')
//              .split(',')
//              .map(value => value.toString.toDouble)
//
//            var object1DominatesObject2 = true
//    TODO we can use this algorithm for third question
//      // TODO try for each partition
//      //      todo try reduce by key with + for oid1
//      //   TODO   grid and count how many points are in each cell in order to reduce number of rows if the two previous lines dont work
//            object1.zip(object2).foreach { case (object1value, object2value) =>
//              if (object1value.toString.toDouble > object2value.toString.toDouble) {
//                object1DominatesObject2 = false
//              }
//            }
//
//            if (object1DominatesObject2) {
//              val updatedMap = Map(s"${row(0)}" -> 1)
//
//              hashMapAccumulator.add(updatedMap)
//            }
//    })
//////    val dcRDD = joinedRDD
////////      .rdd
////////        .map(row => (row(1), ((row(0), row(3)), (row(4), row(7))))
////////      .partitionBy(new CustomPartitioner(NUM_OF_CORES.get.toInt))
//////
////////      .flatMap { case (key, iterable) => iterable.map(value => value)}
//////      .filter{ case (key, value) =>  value._1._1(2).toInt < value._2._1(1).toInt}
////////      .partitionBy(new CustomPartitioner(NUM_OF_CORES.get.toInt))
//////      .map{ case (key, value) => (value._1._1(0), value._1._2, value._2._1(0), value._2._2)}
////////      .partitionBy(new CustomPartitioner(NUM_OF_CORES.get.toInt))
//////    // 4.5 Skyband and Dominating Objects Computation
//////    // dcRDD schema: OID1, OID2 for object pairs to check dominance
////////    val inputFilePairedRDD = inputFileRDDWithId
////////      .map(row => (row.split(',')(0), row.split(',').tail))
//////
////////    val joinedRDDWithObjectVectors = dcRDD
////////      .join(inputFilePairedRDD)
////////      .map { row => {
////////        (row._2._1, (row._1, row._2._2))
////////      }
////////      }
////////      .join(inputFilePairedRDD)
////////      .map { row => {
////////        Row(row._1, row._2._2, row._2._1._1, row._2._1._2)
////////      }
////////      }
////////
////////    //    TODO see how many partitions joinedRDDWithObjectVectors has and set them equal to the number of executors
////////
////////    //    val joinedRDDWithObjectVectorsRepartioned = joinedRDDWithObjectVectors.repartition(NUM_OF_CORES.get.toInt)
////////
////////    println(joinedRDDWithObjectVectors.getNumPartitions)
//////    // the first object should be checked if it dominates the second
////////    dcRDD
//    topKDominant.rdd.saveAsTextFile("./test")
    topKDominant.rdd.foreach(row => {
//      println(row)
      val object2 = row(7)
        .toString
        .filter(_ != '[')
        .filter(_ != ']')
        .split(',')
        .map(value => value.toDouble)
//
      val object1 = row(3)
        .toString
        .filter(_ != '[')
        .filter(_ != ']')
        .split(',')
        .map(value => value.toDouble)
//
      var object1DominatesObject2 = true

//      grid and if the object 2 is in a cell that object1 dominates for sure do not do computations
      object1.zip(object2).foreach { case (object1value, object2value) =>
        if (object1value.toString.toDouble > object2value.toString.toDouble) {
          object1DominatesObject2 = false
        }
      }

      if (object1DominatesObject2) {
        val updatedMap = Map(s"${row(0)}" -> 1)

        hashMapAccumulator.add(updatedMap)
      }
    })
////////16000000
//////    // NOTE we can keep a data structure (like a priority queue) of K positions and iterate over the values one time instead of sorting
    val finalHashMap = hashMapAccumulator.value.flatten.groupBy(_._1).mapValues(_.map(_._2).sum)
    val topKeys = finalHashMap.toList.sortBy(-_._2).take(K.get.toInt).map(_._1)
//    println(joinedRDD.count())
//    println(joinedRDD.take(1).mkString("Array(", ", ", ")"))

//    topKDominant.sav
    println(s"Keys with the highest $K values: ${topKeys}")

    val endTime = System.nanoTime()
    val elapsedTime = (endTime - startTime) / 1e9 // Convert nanoseconds to seconds
    println(s"Elapsed time: $elapsedTime seconds")
//    val result = topKDominant.rdd.mapPartitionsWithIndex {
//      (index, iterator) => Iterator((index, iterator.size))
//    }
//
//    result.collect().foreach(println)

  }
}