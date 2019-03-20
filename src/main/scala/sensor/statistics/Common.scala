package sensor.statistics

import java.nio.file.Path

import com.github.tototoshi.csv.CSVReader

import scala.util.Try

object Common {

  type Row = Map[String, String]
  type SensorId = String
  type Humidity = Int
  type Count = BigInt
  val Count = BigInt
  type Total = BigInt
  val Total = BigInt

  val SensorId = "sensor-id"
  val Humidity = "humidity"

  val help =
    """
      |A command line program that calculates statistics from humidity sensor data.
      |
      |## Input
      |
      |- Program takes one argument: a path to directory
      |- Directory contains many CSV files (*.csv), each with a daily report from one group leader
      |- Format of the file: 1 header line + many lines with measurements
      |- Measurement line has sensor id and the humidity value
      |- Humidity value is integer in range `[0, 100]` or `NaN` (failed measurement)
      |- The measurements for the same sensor id can be in the different files
    """.stripMargin

  def show(files: Number, processed: Number, failed: Number)(results: => String): String =
    s"""
       |Num of processed files: ${files}
       |Num of processed measurements: ${processed}
       |Num of failed measurements: ${failed}
       |
       |Sensors with highest avg humidity:
       |
       |sensor-id,min,avg,max
       |${results}
       |""".stripMargin

  def csv(file: Path): CSVReader = CSVReader.open(file.toFile)

  def desc[A: Ordering]: Ordering[A] = Ordering[A].reverse

  def showNaN[A](a: => Option[A]): String = a.fold("NaN")(_.toString)

  def tryOpt[A, B](f: A => B)(a: A): Option[B] = Try(f(a)).toOption

  def autoClose[A <: AutoCloseable, B](a: A)(f: A => B): B = try f(a) finally a.close()

  def liftOp[A](op: (A, A) => A)(x: Option[A], y: Option[A]): Option[A] =
    (for (a <- x; b <- y) yield op(a, b)).orElse(x).orElse(y)

}
