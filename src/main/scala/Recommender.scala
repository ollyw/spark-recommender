package org.my

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._


object Recommender extends App {
  val parallelism = 4
  val sparkConf = new SparkConf().
                        setAppName("Spark Recommender").
                        setSparkHome("~/Documents/Spark/spark-1.1.0/").
                        setMaster(s"local[$parallelism]")

  val outputDir = new File("data/output")
  deleteRecursively(outputDir)

  val sc = new SparkContext(sparkConf)

  val startTime = System.nanoTime()
  val rows = sc.textFile("data/links-simple-sorted-1000.txt", 2)
  val linePattern = """(\d+)""".r
  val recommendationsPerUser = 5

  val userVectors: RDD[(Long, Seq[(Int, Double)])] = rows.map { r =>
    val numbers = linePattern.findAllIn(r).toIterable
    val user = numbers.head.toLong
    val items = numbers.tail.map(i => i.toInt -> 1.0).toArray
    user -> items
  }

  val allCooccurrences = userVectors.flatMap { case (_, prefs) =>
    // Output combinations both ways, e.g. Array(1, 2) => Array((1, 2), (2, 1))
    val prefItems = prefs.map(_._1)
    (prefItems.combinations(2) ++ prefItems.reverse.combinations(2)).map { x => x(0) -> (x(1) -> 1L)}
  }

  def countItemFrequency(pairs: Iterable[(Int, Long)]): Iterable[(Int, Long)] = {
    pairs.groupBy { case (k, _) => k}
      .map { case (k, v) => k -> v.map(_._2).sum}
  }

  val cooccurrencesWithFrequency = allCooccurrences.groupByKey().map { case (key, values) => key -> countItemFrequency(values)}.cache()

  val userAndCooccurrenceCols = userVectors.map { case (user, prefs) =>
    val relevantCoocurrenceCols = cooccurrencesWithFrequency.filter { case (k, _) => prefs.indices.contains(k)}
    user -> (prefs -> relevantCoocurrenceCols)
  }

  // For all co-occurrences, times the user's preference with the number of co-occurrences, sum the total and store against the item
  val userPreferences: RDD[(Long, Iterable[(Int, Double)])] = userVectors.map { case (user, prefs) =>
    val prefMap: Map[Int, Double] = prefs.toMap.withDefaultValue(0.0)
    val itemRatings = cooccurrencesWithFrequency.map { case (item, cooccurrenceCols) =>
      val score = cooccurrenceCols.foldLeft(0.0) { case (acc, (cooc, freq)) => acc + (prefMap(cooc) * freq)}
      item -> score
    }
    val recommendations = itemRatings
      .filter { case (item, rating) => prefMap(item) == 0.0 && rating > 0.0}.sortBy({ case (k, v) => v}, true)
      .take(recommendationsPerUser)
    user -> recommendations
  }

  userPreferences.saveAsTextFile(outputDir.getCanonicalPath)

  val durationSecs = (System.nanoTime() - startTime) / (1000 * 1000 * 1000)
  println(s"Completed in $durationSecs seconds")

  Console.readLine()

  sc.stop()

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) file.listFiles().map(deleteRecursively)
    file.delete()
  }

  def timeSecs(block: => Any): Long = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    (t1 - t0) / (1000 * 1000)
  }
}
