package sensor.statistics.catseff

import java.nio.file.{Files, Path, Paths}

import cats._
import cats.data._
import cats.effect._
import cats.implicits._
import sensor.statistics.Common
import sensor.statistics.Common._
import sensor.statistics.catseff.Monoids._

import scala.compat.java8.StreamConverters._

object SensorCats extends IOApp {

  type Parser[A] = Kleisli[Option, Row, A] //  (Row => Option[A])

  type SensorData = (SensorId, Option[Humidity])

  implicit lazy val sensorStatMonoid: Monoid[SensorStat] =
    (Monoid[Option[Min[Humidity]]], Monoid[Option[Max[Humidity]]], Monoid[Count], Monoid[Total])
      .imapN(SensorStat.apply)(Function.unlift(SensorStat.unapply))

  implicit lazy val fileStatMonoid: Monoid[FileStat] =
    (Monoid[Count], Monoid[Count], Monoid[Map[SensorId, SensorStat]])
      .imapN(FileStat.apply)(Function.unlift(FileStat.unapply))

  implicit lazy val sensorStatShow = Show.show[SensorStat](showSensorStat)
  implicit lazy val dirStatShow = Show.show[DirStat](showDirStat)

  def field(field: String): Parser[String] = Kleisli(_.get(field))

  lazy val readSensor: Parser[SensorData] =
    field(SensorId) &&& field(Humidity).map(tryOpt(_.toInt))

  override def run(args: List[String]): IO[ExitCode] = (args match {
    case List(path) => showStat(Paths.get(path)).map(ExitCode.Success -> _)
    case _ => IO(Common.help).map(ExitCode.Error -> _)
  }).map(_.map(println)._1)

  def showStat(dir: Path): IO[String] = readDir(dir).map(_.show)

  case class SensorStat(min: Option[Min[Humidity]], max: Option[Max[Humidity]], count: Count, total: Total)

  case class FileStat(measurements: Count, failed: Count, sensors: Map[SensorId, SensorStat])

  case class DirStat(files: Count, measurements: Count, failed: Count, sensors: Seq[(SensorId, SensorStat)])

  def showSensorStat(s: SensorStat): String =
    s"${showNaN(s.min.map(_.min))},${showNaN(sensorAvg(s))},${showNaN(s.max.map(_.max))}"

  def showDirStat(s: DirStat) = Common.show(s.failed, s.measurements,
    s.failed)(s.sensors.map(s => s"${s._1},${s._2.show}").mkString("\n"))

  def sensorAvg(s: SensorStat): Option[Humidity] =
    if (s.count == 0) none else (s.total / s.count).some.map(_.toInt)

  def fileStat(sensorId: String, humidity: Option[Humidity]) = FileStat(Count(1),
    Count(humidity.fold(1)(_ => 0)), Map(sensorId ->
      SensorStat(humidity.map(Min(_)), humidity.map(Max(_)),
        Count(humidity.fold(0)(_ => 1)), Total(humidity.fold(0)(identity)))))

  def dirStat(files: Count, stat: FileStat) = DirStat(files,
    stat.measurements, stat.failed, stat.sensors.toStream
      .sortBy(_.map(sensorAvg)._2)(desc).toVector)

  def readFile(file: Path) = IO(Common.csv(file)).bracket {
    IO(_).map(_.iteratorWithHeaders.flatMap(readSensor(_))
      .toStream.foldMap((fileStat _).tupled))
  }(IO(_).map(_.close))

  def readDir(dir: Path) = IO(Files.list(dir)).bracket {
    IO(_).map(_.toScala[Stream]).flatMap(_.traverse(readFile))
      .map(_.foldMap((Count(1), _))).map((dirStat _).tupled)
  }(IO(_).map(_.close))

}
