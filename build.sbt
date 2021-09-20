organization := "com.github.pilillo"
name := "gilberto"
version := "0.1"

scalaVersion := "2.12.12"
val sparkVersion = "3.0.3"

resolvers ++= Seq(
  "mvnrepository" at "https://mvnrepository.com/artifact/",
  "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Spark Snapshot Repository" at "https://repository.apache.org/snapshots",
)

libraryDependencies ++= Seq(
  "com.github.scopt" %% "scopt" % "3.7.1",
  "org.scalacheck" %% "scalacheck" % "1.14.1" % Test,
  "org.apache.spark"  %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "commons-validator" % "commons-validator" % "1.7",
  "com.holdenkarau" %% "spark-testing-base" % s"3.0.2_1.1.0" % Test,
  "com.amazon.deequ" % "deequ" % "1.2.2-spark-3.0"
)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

// https://github.com/holdenk/spark-testing-base#special-considerations
parallelExecution in Test := false