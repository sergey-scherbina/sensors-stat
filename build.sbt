name := "sensor-statistics"

version := "0.1"

scalaVersion := "2.12.8"

mainClass in assembly := Some("sensor.statistics.SensorsStats")

scalacOptions += "-Ypartial-unification"

libraryDependencies := Seq(
  "org.scalatest" %% "scalatest" % "3.0.6" % "test",
  "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0",
  "com.github.tototoshi" %% "scala-csv" % "1.3.5",
  "org.typelevel" %% "cats-core" % "1.5.0",
  "org.typelevel" %% "cats-effect" % "1.2.0"
)
