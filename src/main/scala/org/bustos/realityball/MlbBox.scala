package org.bustos.realityball

import java.io._
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

  case class BatterLinescore(player: Player, starter: Boolean, ab: Int, r: Int, h: Int, rbi: Int, bb: Int, so: Int, log: Int, avg: Double)
  case class PitcherLinescore(player: Player, ip: Double, h: Int, r: Int, er: Int, bb: Int, so: Int, hr: Int, era: Double)
  case class GameInfo(id: String, pitchesStrikes: String, groundFly: String, batterFaced: String, umpires: String,
                      temp: String, sky: String, windSpeed: String, windDir: String, time: String, attendance: String, venue: String)

}

class MlbBox(val game: Game) extends Chrome {

  import MlbBox._

  implicitlyWait(Span(20, Seconds))

  val realityballData = new RealityballData
  val logger = LoggerFactory.getLogger(getClass)

  val mlbIdExpression = "(.*)=(.*)".r
  val gameInfoExpression = ".*Pitches-strikes: (.*)Groundouts-flyouts: (.*)Batters faced: (.*)Umpires: (.*)Weather: (.*)Wind: (.*)T: (.*)Att: (.*)Venue: (.*)Compiled.*".r
  val windExpression = """(.*) mph, (.*)\.""".r
  val skyExpression = """(.*) degrees, (.*)\.""".r
  val timeOfGameExpression = """(.*?):(.*?)[\. ](.*)""".r

  val date = CcyymmddSlashDelimFormatter.parseDateTime(game.date)
  val mlbHomeTeam = realityballData.mlbComIdFromRetrosheet(game.homeTeam)
  val mlbVisitingTeam = realityballData.mlbComIdFromRetrosheet(game.visitingTeam)

  logger.info("********************************")
  logger.info("*** Retrieving box results for " + game.visitingTeam + " @ " + game.homeTeam + " on " + CcyymmddDelimFormatter.print(date))
  logger.info("********************************")

  val gameId = game.homeTeam.toUpperCase + CcyymmddFormatter.print(date)

  val fileName = DataRoot + "gamedayPages/" + date.getYear + "/" + game.visitingTeam + "_" + game.homeTeam + "_" + CcyymmddFormatter.print(date) + "_box.html"

  if (new File(fileName).exists) {
    val caps = DesiredCapabilities.chrome;
    caps.setCapability("chrome.switches", Array("--disable-javascript"));
    go to "file://" + fileName
  } else {
    val host = GamedayURL
    go to host + "mlb/gameday/index.jsp?gid=" + CcyymmddDelimFormatter.print(date) + "_" + game.visitingTeam.toLowerCase + "mlb_" + game.homeTeam.toLowerCase + "mlb_1&mode=box"
  }

  def playerFromMlbUrl(mlbUrl: WebElement): Player = {
    val localYear = if (YearFormatter.print(date) == "2015") "2014" else YearFormatter.print(date)
    val url = mlbUrl.findElement(new ByTagName("a")).getAttribute("href")
    url match {
      case mlbIdExpression(urlString, mlbId) => realityballData.playerFromMlbId(mlbId, localYear)
      case _                                 => throw new IllegalStateException("No player found")
    }
  }

  def batterLinescore(result: List[BatterLinescore], row: WebElement): List[BatterLinescore] = {
    if (!row.getAttribute("textContent").contains("AVG") && !row.getAttribute("textContent").contains("Totals")) {
      row match {
        case thRow: RemoteWebElement => {
          val details = thRow.findElementsByTagName("td")
          val player = playerFromMlbUrl(details(0))
          val playerClass = details(0).getAttribute("class")
          val linescore = BatterLinescore(
            player,
            !playerClass.contains("ph"),
            details(1).getAttribute("textContent").toInt,
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
      case _ => List.empty[BatterLinescore]
    }
  }
  def pitcherLinescore(result: List[PitcherLinescore], row: WebElement): List[PitcherLinescore] = {
    if (!row.getAttribute("textContent").contains("ERA") && !row.getAttribute("textContent").contains("Totals")) {
      row match {
        case thRow: RemoteWebElement => {
          val details = thRow.findElementsByTagName("td")
          val player = playerFromMlbUrl(details(0))
          val linescore = PitcherLinescore(
            player,
            details(1).getAttribute("textContent").toDouble,
            details(2).getAttribute("textContent").toInt,
            details(3).getAttribute("textContent").toInt,
            details(4).getAttribute("textContent").toInt,
            details(5).getAttribute("textContent").toInt,
            details(6).getAttribute("textContent").toInt,
            details(7).getAttribute("textContent").toInt,
            if (!details(8).getAttribute("textContent").contains("-")) details(8).getAttribute("textContent").toDouble else 0.0)
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
      case _ => List.empty[PitcherLinescore]
    }
  }

  val gameInfo: GameInfo = {
    find("game-info-container") match {
      case Some(x) => {
        val gameInfoText = x.underlying.getAttribute("textContent")
        gameInfoText.replace("\n", "") match {
          case gameInfoExpression(pitchesStrikes, groundFly, battersFaced, umpires, weather, wind, time, attendance, venue) => {
            val timeOfGame = time match {
              case timeOfGameExpression(hourCount, minuteCount, delayCount) => hourCount.toDouble * 60 + minuteCount.toDouble
              case _ => 0.0
            }
            val temp = weather match {
              case skyExpression(temp, sky) => temp
              case _                        => ""
            }
            val sky = weather match {
              case skyExpression(temp, sky) => sky
              case _                        => ""
            }
            val windSpeed = wind match {
              case windExpression(speed, dir) => speed
              case _                          => ""
            }
            val windDirection = wind match {
              case windExpression(speed, dir) => dir
              case _                          => ""
            }
            GameInfo(gameId, pitchesStrikes, groundFly, battersFaced, umpires, temp, sky, windSpeed, windDirection, timeOfGame.toString, attendance.replaceAll(",", ""), venue.split(" ")(0))
          }
        }
      }
      case _ => GameInfo("", "", "", "", "", "", "", "", "", "", "", "")
    }
  }

  val pitchingResults: List[(WebElement, WebElement)] = {
    find("linescore-wrapper") match {
      case Some(x) => x.underlying match {
        case y: RemoteWebElement => y.findElementsByTagName("dt").toList.zip(y.findElementsByTagName("dd").toList)
        case _                   => List.empty[(WebElement, WebElement)]
      }
      case _ => List.empty[(WebElement, WebElement)]
    }
  }

  def pitcherForType(resultType: String): Player = {
    val pitcherElement = pitchingResults.filter {
      case (result, player) =>
        result match {
          case x: RemoteWebElement => x.getAttribute("textContent").contains(resultType + ":")
          case _                   => false
        }
    }
    if (pitcherElement.isEmpty) null
    else {
      playerFromMlbUrl(pitcherElement.head._2)
    }
  }

  val winningPitcher = pitcherForType("W")
  val losingPitcher = pitcherForType("L")
  val savingPitcher = pitcherForType("SV")

  val awayBatterLinescores = batterLinescores("away-team-batter")
  val homeBatterLinescores = batterLinescores("home-team-batter")
  val awayPitcherLinescores = pitcherLinescores("away-team-pitcher")
  val homePitcherLinescores = pitcherLinescores("home-team-pitcher")
  val awayStartingLineup = awayBatterLinescores.filter { _.starter }
  val homeStartingLineup = homeBatterLinescores.filter { _.starter }

  if (!(new File(fileName)).exists) {
    val writer = new FileWriter(new File(fileName))
    writer.write(pageSource)
    writer.close
  }

  quit
}
