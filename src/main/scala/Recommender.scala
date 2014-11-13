package org.my

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
  val rows = sc.parallelize(Seq((-239L, "590 22 9059"), (-240L, "22 100"), (-241L, "22 100 590")))

  val userVectors = rows.map { case (user, prefsLine) =>
    val prefs = prefsLine.split(" ").map(_.toInt)
    val scores = Array.fill(prefs.size)(1.0)
    (user, new SparseVector(prefs.size, prefs, scores))
  }

  val allCooccurences = userVectors.flatMap { case (_, vector) =>
    // Output combinations both ways, e.g. Array(1, 2) => Array((1, 2), (2, 1))
    (vector.indices.combinations(2) ++ vector.indices.reverse.combinations(2)).map { x => x(0) -> (x(1) -> 1L) }
  }

  val cooccurencesWithFrequency = allCooccurences.groupByKey().map { case (key, values) => key -> countItemFrequency(values) }

  def countItemFrequency(pairs: Iterable[(Int, Long)]): Iterable[(Int, Long)] = {
    pairs.groupBy { case (k, _) => k }
      .map { case (k, v) => k -> v.map(_._2).sum }
  }

  println(userVectors.collect.map { case (k, v) => println(k + ":" + v.toString)})
  //println(cooccurencesWithFrequency.collect.length)
  cooccurencesWithFrequency.foreach(println)
  sc.stop()
}
