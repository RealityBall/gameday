package org.bustos.realityball

import org.joda.time._
import org.joda.time.format._
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
  case class GameInfo(id: String, pitchesStrikes: String, groundFly: String, batterFaced: String, umpires: String, weather: String, wind: String, time: String, attendance: String, venue: String)

}

class MlbBox(date: DateTime, awayTeam: String, homeTeam: String) extends Chrome {

  import MlbBox._

  implicitlyWait(Span(20, Seconds))

  val realityballData = new RealityballData
  val logger = LoggerFactory.getLogger(getClass)
  val mlbIdExpression: Regex = "(.*)=(.*)".r
  val gameInfoExpression: Regex = ".*Pitches-strikes: (.*)Groundouts-flyouts: (.*)Batters faced: (.*)Umpires: (.*)Weather: (.*)Wind: (.*)T: (.*)Att: (.*)Venue: (.*)".r

  logger.info("********************************")
  logger.info("*** Retrieving box results for " + awayTeam + " @ " + homeTeam + " on " + CcyymmddDelimFormatter.print(date))
  logger.info("********************************")

  val gameId = homeTeam.toUpperCase + CcyymmddFormatter.print(date)

  val host = GamedayURL
  go to host + "mlb/gameday/index.jsp?gid=" + CcyymmddDelimFormatter.print(date) + "_" + awayTeam.toLowerCase + "mlb_" + homeTeam.toLowerCase + "mlb_1&mode=box"

  var pitchCount = 0
  var awayBatterLinescores = List.empty[BatterLinescore]
  var homeBatterLinescores = List.empty[BatterLinescore]
  var awayPitcherLinescores = List.empty[PitcherLinescore]
  var homePitcherLinescores = List.empty[PitcherLinescore]

  def playerFromMlbUrl(mlbUrl: WebElement): Player = {
    val url = mlbUrl.findElement(new ByTagName("a")).getAttribute("href")
    url match {
      case mlbIdExpression(urlString, mlbId) => realityballData.playerFromMlbId(mlbId, YearFormatter.print(date))
      case _                                 => throw new IllegalStateException("No player found")
    }
  }

  def batterLinescore(result: List[BatterLinescore], row: WebElement): List[BatterLinescore] = {
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

  def batterLinescores(linescoreType: String): List[BatterLinescore] = {
    find(linescoreType) match {
      case Some(x) => x.underlying match {
        case linescoresElement: RemoteWebElement => {
          linescoresElement.findElementsByTagName("tr") match {
            case batters: java.util.List[WebElement] => batters.foldLeft(List.empty[BatterLinescore])((x, y) => batterLinescore(x, y))
            case _                                   => throw new IllegalStateException("No linescore entries found")
          }
        }
      }
      case _ => throw new IllegalStateException("No linescores found")
    }
  }
  def pitcherLinescore(result: List[PitcherLinescore], row: WebElement): List[PitcherLinescore] = {
    if (!row.getAttribute("textContent").contains("ERA") && !row.getAttribute("textContent").contains("Totals")) {
      row match {
        case thRow: RemoteWebElement => {
          val details = thRow.findElementsByTagName("td")
          val player = playerFromMlbUrl(details(0))
          val linescore = PitcherLinescore(playerFromMlbUrl(details(0)).id, details(1).getAttribute("textContent").toDouble,
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

  def pitcherLinescores(linescoreType: String): List[PitcherLinescore] = {
    find(linescoreType) match {
      case Some(x) => x.underlying match {
        case linescoresElement: RemoteWebElement => {
          linescoresElement.findElementsByTagName("tr") match {
            case pitchers: java.util.List[WebElement] => pitchers.foldLeft(List.empty[PitcherLinescore])((x, y) => pitcherLinescore(x, y))
            case _                                    => throw new IllegalStateException("No linescore entries found")
          }
        }
      }
      case _ => throw new IllegalStateException("No linescores found")
    }
  }
  val gameInfo: GameInfo = {
    find("game-info-container") match {
      case Some(x) => {
        val gameInfoText = x.underlying.getAttribute("textContent")
        gameInfoText.replace("\n", "") match {
          case gameInfoExpression(pitchesStrikes, groundFly, battersFaced, umpires, weather, wind, time, attendance, venue) => {
            GameInfo(gameId, pitchesStrikes, groundFly, battersFaced, umpires, weather, wind, time, attendance, venue)
          }
        }
      }
      case _ => throw new IllegalStateException("No game-info-container found")
    }
  }

  awayBatterLinescores = batterLinescores("away-team-batter")
  homeBatterLinescores = batterLinescores("home-team-batter")
  awayPitcherLinescores = pitcherLinescores("away-team-pitcher")
  homePitcherLinescores = pitcherLinescores("home-team-pitcher")

  quit
}
