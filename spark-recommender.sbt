name := "Spark Recommender"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core" % "1.1.0",
  "org.apache.spark"  %% "spark-mllib" % "1.1.0"
)

scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation")
