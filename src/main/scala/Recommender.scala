package org.my

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.SparseVector


object Recommender extends App {
  val parallelism = 4
  val sparkConf = new SparkConf().
                        setAppName("Spark Recommender").
                        setSparkHome("~/Documents/Spark/spark-1.1.0/").
                        setMaster(s"local[$parallelism]")

  val sc = new SparkContext(sparkConf)
  val rows = sc.parallelize(Seq((-239L, "590 22 9059"), (-240L, "22 100"), (-241L, "22 100 590")), 2)

  val userVectors: RDD[(Long, Array[(Int, Double)])] = {
    rows.map { case (user, prefsLine) =>
      val prefs = prefsLine.split(" ").map(_.toInt).map(item => item -> 1.0)
      (user, prefs)
    }
  }

  val allCooccurrences = userVectors.flatMap { case (_, prefs) =>
    // Output combinations both ways, e.g. Array(1, 2) => Array((1, 2), (2, 1))
    val prefItems = prefs.map(_._1)
    (prefItems.combinations(2) ++ prefItems.reverse.combinations(2)).map { x => x(0) -> (x(1) -> 1L) }
  }

  val cooccurrencesWithFrequency = allCooccurrences.groupByKey().map { case (key, values) => key -> countItemFrequency(values) }

  def countItemFrequency(pairs: Iterable[(Int, Long)]): Iterable[(Int, Long)] = {
    pairs.groupBy { case (k, _) => k }
      .map { case (k, v) => k -> v.map(_._2).sum }
  }

  val userAndCooccurrenceCols = userVectors.map { case (user, prefs) =>
    val relevantCoocurrenceCols = cooccurrencesWithFrequency.filter { case (k, _) => prefs.indices.contains(k) }
    user -> (prefs -> relevantCoocurrenceCols)
  }

  // For all co-occurrences, times the user's preference with the number of co-occurrences, sum the total and store against the item
  val userPreferences: RDD[(Long, Iterable[(Int, Double)])] = userVectors.map { case (user, prefs) =>
    val prefMap: Map[Int, Double] = prefs.toMap.withDefaultValue(0.0)
    val itemRatings = cooccurrencesWithFrequency.map { case (item, cooccurrenceCols) =>
      val score = cooccurrenceCols.foldLeft(0.0) { case (acc, (cooc, freq)) => acc + (prefMap(cooc) * freq)}
      item -> score
    }
    val recommendations = itemRatings.filter { case (item, rating) => prefMap(item) == 0.0 && rating > 0.0 }.collect
    user -> recommendations.sortBy(_._2)(Ordering[Double].reverse)
  }

  println(userVectors.collect.map { case (k, v) => println(k + ":" + v.toString)})
  cooccurrencesWithFrequency.foreach(println)
  println("Cols")
  userPreferences.foreach { case (user, prefs) => println(user + "/" + prefs.mkString(","))}
  sc.stop()
}
