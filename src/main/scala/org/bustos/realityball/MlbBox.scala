package org.bustos.realityball

import java.util.Date
import java.text.SimpleDateFormat;
import org.scalatest._
import selenium._
import org.scalatest.time.{ Span, Seconds }
import org.openqa.selenium.support.ui.{ WebDriverWait, ExpectedCondition }
import org.openqa.selenium._
import org.openqa.selenium.By._
import remote._
import htmlunit._
import scala.util.matching.Regex
import scala.collection.JavaConversions._
import org.slf4j.LoggerFactory
import RealityballRecords._
import RealityballConfig._

object MlbBox {
  case class BatterLinescore(id: String, ab: Int, r: Int, h: Int, rbi: Int, bb: Int, so: Int, log: Int, avg: Double)
  case class PitcherLinescore(id: String, ip: Double, h: Int, r: Int, er: Int, bb: Int, so: Int, hr: Int, era: Double)

  val dateFormat = new SimpleDateFormat("yyyy_MM_dd");
  val yearFormat = new SimpleDateFormat("yyyy");
}

class MlbBox(date: Date, awayTeam: String, homeTeam: String) extends Chrome {

  import MlbBox._

  implicitlyWait(Span(20, Seconds))

  val realityballData = new RealityballData
  val logger = LoggerFactory.getLogger(getClass)
  val mlbIdExpression: Regex = "(.*)=(.*)".r

  logger.info("********************************")
  logger.info("*** Retrieving box results for " + awayTeam + " @ " + homeTeam + " on " + dateFormat.format(date))
  logger.info("********************************")

  val host = GamedayURL
  go to host + "index.jsp?gid=" + dateFormat.format(date) + "_" + awayTeam.toLowerCase + "mlb_" + homeTeam.toLowerCase + "mlb_1&mode=box"

  var pitchCount = 0
  var awayBatterLinescores = List.empty[BatterLinescore]
  var homeBatterLinescores = List.empty[BatterLinescore]
  var awayPitcherLinescores = List.empty[PitcherLinescore]
  var homePitcherLinescores = List.empty[PitcherLinescore]

  def playerFromMlbUrl(mlbUrl: WebElement): Player = {
    val url = mlbUrl.findElement(new ByTagName("a")).getAttribute("href")
    url match {
      case mlbIdExpression(urlString, mlbId) => realityballData.playerFromMlbId(mlbId, yearFormat.format(date))
      case _                                 => throw new IllegalStateException("No player found")
    }
  }

  def linescore(result: List[BatterLinescore], row: WebElement): List[BatterLinescore] = {
    if (!row.getAttribute("textContent").contains("AVG") && !row.getAttribute("textContent").contains("Totals")) {
      row match {
        case thRow: RemoteWebElement => {
          val details = thRow.findElementsByTagName("td")
          val player = playerFromMlbUrl(details(0))
          val linescore = BatterLinescore(playerFromMlbUrl(details(0)).id, details(1).getAttribute("textContent").toInt,
            details(2).getAttribute("textContent").toInt,
            details(3).getAttribute("textContent").toInt,
            details(4).getAttribute("textContent").toInt,
            details(5).getAttribute("textContent").toInt,
            details(6).getAttribute("textContent").toInt,
            details(7).getAttribute("textContent").toInt,
            details(8).getAttribute("textContent").toDouble)
          logger.info(linescore.toString)
          linescore :: result
        }
        case _ => result
      }
    } else result
  }

  def linescores(linescoreType: String): List[BatterLinescore] = {
    find(linescoreType) match {
      case Some(x) => x.underlying match {
        case linescoresElement: RemoteWebElement => {
          linescoresElement.findElementsByTagName("tr") match {
            case batters: java.util.List[WebElement] => batters.foldLeft(List.empty[BatterLinescore])((x, y) => linescore(x, y))
            case _                                   => throw new IllegalStateException("No linescore entries found")
          }
        }
      }
      case _ => throw new IllegalStateException("No linescores found")
    }
  }
  awayBatterLinescores = linescores("away-team-batter")
  homeBatterLinescores = linescores("home-team-batter")
  quit
}
