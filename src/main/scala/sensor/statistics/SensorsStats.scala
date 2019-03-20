package sensor.statistics

import java.nio.file.{Files, Path, Paths}

import sensor.statistics.Common._

import scala.compat.java8.StreamConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try

object SensorsStats extends App {

  private implicit lazy val executor: ExecutionContextExecutor = ExecutionContext.global

  Try(args(0)).map(Paths.get(_)).toOption
    .fold(println(help))(run(_).fold(
      _.printStackTrace(), println))

  def run(dir: Path)(implicit e: ExecutionContext = executor): Try[String] =
    Results.results(dir).map(showResults)

  def showResults(r: Results): String = Common.show(r.files,
    r.measurements, r.failed)(r.sort().mkString("\n"))

  case class Measurement(sensor: SensorId, humidity: Option[Humidity]) {
    @inline def isFailed(): Boolean = humidity.isEmpty
  }

  object Measurement {
    def parse(line: Row): Option[Measurement] =
      for (s <- line.get(SensorId); h <- line.get(Humidity))
        yield Measurement(s, Try(h.toInt).toOption)
  }

  case class Stats(sensor: SensorId,
                   min: Option[Humidity] = Option.empty,
                   max: Option[Humidity] = Option.empty,
                   count: Count = 0, total: Total = 0) {

    override def toString(): String =
      s"$sensor,${showNaN(min)},${showNaN(avg())},${showNaN(max)}"

    def avg(): Option[Humidity] = if (count == 0) Option.empty else Option(total / count).map(_.toInt)

    def add(m: Measurement): Stats = {
      assert(sensor == m.sensor, s"Wrong measure for sensor [${m.sensor}]. Should be [$sensor]")
      m.humidity.map(h => copy(
        min = min.map(Math.min(h, _)).orElse(Option(h)),
        max = max.map(Math.max(h, _)).orElse(Option(h)),
        count = count + 1, total = total + h
      )).getOrElse(this)
    }

    def merge(s: Stats): Stats = {
      assert(sensor == s.sensor, s"Wrong stats for sensor [${s.sensor}]. Should be [$sensor]")
      copy(
        count = count + s.count,
        total = total + s.total,
        min = liftOp(Math.min)(min, s.min),
        max = liftOp(Math.max)(max, s.max)
      )
    }
  }

  case class Results(files: Int = 1, measurements: Int = 0, failed: Int = 0,
                     stats: Map[SensorId, Stats] = Map.empty) {

    def sort(): Stream[Stats] = stats.values.toStream
      .sortBy(_.avg().getOrElse(Int.MinValue))(desc)

    def add(m: Measurement): Results = copy(
      measurements = measurements + 1,
      failed = failed + (if (m.isFailed()) 1 else 0),
      stats = stats.updated(m.sensor, stats.getOrElse(m.sensor, Stats(m.sensor)).add(m))
    )

    def merge(r: Results): Results = copy(
      files = files + r.files,
      measurements = measurements + r.measurements,
      failed = failed + r.failed,
      stats = (stats.toSeq ++ r.stats.toSeq).groupBy(_._1)
        .mapValues(_.map(_._2).reduce(_.merge(_)))
    )
  }

  object Results {
    def results(dir: Path)(implicit e: ExecutionContext): Try[Results] =
      Try(Await.result(readDir(dir), Duration.Inf))

    def readFile(path: Path): Results = autoClose(csv(path))(
      _.iteratorWithHeaders.foldLeft(Results())((r, line) =>
        Measurement.parse(line).map(r.add).getOrElse(r)))

    def readDir(dir: Path)(implicit e: ExecutionContext): Future[Results] =
      Future.reduceLeft(Files.list(dir).toScala[List]
        .map(p => Future(readFile(p))))(_.merge(_))
  }

}
