organization := "eu.krcz"

scalaVersion := "2.10.6"

lazy val sparkDeps = Seq(
	"org.apache.spark" %% "spark-streaming" % "1.6.0",
	"org.apache.spark" %% "spark-streaming-kafka" % "1.6.0"
)

libraryDependencies ++= sparkDeps

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.4" % "test"

libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "1.6.0_0.3.0"

fork in run := true

parallelExecution in Test := false
