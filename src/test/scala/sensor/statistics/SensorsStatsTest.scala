package sensor.statistics

import java.nio.file.Paths

import org.scalatest.FunSuite
import sensor.statistics.catseff.SensorCats

import scala.util.Try

class SensorsStatsTest extends FunSuite {

  val path = Paths.get("./src/test/resources/example")

  val expected =
    """|
       |Num of processed files: 2
       |Num of processed measurements: 7
       |Num of failed measurements: 2
       |
       |Sensors with highest avg humidity:
       |
       |sensor-id,min,avg,max
       |s2,78,82,88
       |s1,10,54,98
       |s3,NA,NA,NA
       |""".stripMargin

  test("example") {
    assert(Try(expected) == SensorsStats.run(path).map(_.toString()))
  }

  test("cats") {
    assert(expected == SensorCats.showStat(path).unsafeRunSync())
  }

}
