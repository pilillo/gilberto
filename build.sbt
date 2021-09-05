organization := "com.github.pilillo"
name := "gilberto"
version := "0.1"

//scalaVersion := "2.12.0"
// https://github.com/awslabs/deequ/issues/193
scalaVersion := "2.11.0"

//val sparkVersion = "3.0.1"
val sparkVersion = "2.4.7"

resolvers ++= Seq(
  "mvnrepository" at "https://mvnrepository.com/artifact/",
  "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Spark Snapshot Repository" at "https://repository.apache.org/snapshots",
)

libraryDependencies ++= Seq(
  "com.github.scopt" %% "scopt" % "3.7.1",
  // https://www.scalacheck.org/
  "org.scalacheck" %% "scalacheck" % "1.14.1" % "test",

  "org.apache.spark"  %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",

  // https://mvnrepository.com/artifact/com.holdenkarau/spark-testing-base
  "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % Test,


// https://github.com/awslabs/deequ/issues/193
  // https://github.com/awslabs/deequ/issues/216
  //"com.amazon.deequ" % "deequ" % "1.1.0-spark-3.0.0-scala-2.12",
  //"com.amazon.deequ" % "deequ" % "1.0.7_scala-2.12_spark-3.0.0"
  //"com.amazon.deequ" % "deequ" % "1.0.7_scala-2.12_spark-3.0.0" % "provided" exclude("org.apache.spark", "spark-core_2.11") exclude("org.apache.spark", "spark-sql_2.11")
  "com.amazon.deequ" % "deequ" % "1.1.0_spark-2.4-scala-2.11"
  //"com.amazon.deequ" % "deequ" % "1.1.0_spark-2.2-scala-2.11"
)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

// https://github.com/holdenk/spark-testing-base#special-considerations
parallelExecution in Test := false