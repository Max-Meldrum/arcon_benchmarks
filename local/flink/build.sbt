name := "flink_benchmarks"

version := "1.0"

scalaVersion := "2.12.8"

val flinkVersion = "1.9.0"

fork in run := true

val flinkDependencies: Seq[ModuleID] = Seq(
  "org.apache.flink" %% "flink-scala",
  "org.apache.flink" %% "flink-clients",
  "org.apache.flink" %% "flink-streaming-scala"
).map(_ % flinkVersion)

libraryDependencies ++= flinkDependencies
