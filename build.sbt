organization := "com.github.pilillo"
name := "gilberto"
version := "0.1"

scalaVersion := "2.11.0"
val sparkVersion = "2.4.7"

resolvers ++= Seq(
  "mvnrepository" at "https://mvnrepository.com/artifact/",
  "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Spark Snapshot Repository" at "https://repository.apache.org/snapshots",
)

libraryDependencies ++= Seq(
  "io.lemonlabs" %% "scala-uri" % "1.4.10",

  "com.github.scopt" %% "scopt" % "3.7.1",
  "org.scalacheck" %% "scalacheck" % "1.14.1" % "test",

  "org.apache.spark"  %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",

  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "commons-validator" % "commons-validator" % "1.7",

  // https://mvnrepository.com/artifact/com.holdenkarau/spark-testing-base
  "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % Test,
  "com.amazon.deequ" % "deequ" % "1.1.0_spark-2.4-scala-2.11"
)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

// https://github.com/holdenk/spark-testing-base#special-considerations
parallelExecution in Test := false
