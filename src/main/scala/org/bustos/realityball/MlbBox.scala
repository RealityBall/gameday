package org.bustos.realityball

import java.io._

import org.bustos.realityball.common.RealityballConfig._
import org.bustos.realityball.common.RealityballData
import org.bustos.realityball.common.RealityballRecords._
import org.openqa.selenium.By._
import org.openqa.selenium._
import org.openqa.selenium.remote._
import org.scalatest.selenium._
import org.scalatest.time.{Seconds, Span}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

object MlbBox {

  case class BatterLinescore(player: Player, starter: Boolean, ab: Int, r: Int, h: Int, rbi: Int, bb: Int, so: Int, log: Int, avg: Double)
  case class PitcherLinescore(player: Player, ip: Double, h: Int, r: Int, er: Int, bb: Int, so: Int, hr: Int, era: Double)
  case class GameInfo(id: String, pitchesStrikes: String, groundFly: String, batterFaced: String, umpires: String,
                      temp: String, sky: String, windSpeed: String, windDir: String, time: String, attendance: String, venue: String)

  val mlbIdExpression = "(.*)=(.*)".r
  val mlbIdExpression2 = """(.*)\/(.*)""".r
  val windExpression = """(.*) mph, (.*)\..*First.*""".r
  val skyExpression = """(.*) degrees, (.*)\..*""".r
  val timeOfGameExpression = """(.*?):(.*?)[\. ](.*)""".r
}

class MlbBox(val game: Game) extends Chrome {

  import MlbBox._

  val realityballData = new RealityballData
  val logger = LoggerFactory.getLogger(getClass)

  val date = game.date
  val version_2_format = CcyymmddFormatter.print(date) > "20150401"
  val mlbHomeTeam = realityballData.mlbComIdFromRetrosheet(game.homeTeam)
  val mlbVisitingTeam = realityballData.mlbComIdFromRetrosheet(game.visitingTeam)

  val gameInfoExpression = {
    if (version_2_format) """.*Pitches-strikes: (.*)Groundouts-flyouts: (.*)Batters faced: (.*)Umpires: (.*)Weather: (.*)Wind: (.*)T: (.*)Att: (.*)Venue: (.*)\..*""".r
    else """.*Pitches-strikes: (.*)Groundouts-flyouts: (.*)Batters faced: (.*)Umpires: (.*)Weather: (.*)Wind: (.*)T: (.*)Att: (.*)Venue: (.*)Compiled.*""".r
  }

  logger.info("**********************************************************")
  logger.info("*** Retrieving box results for " + game.visitingTeam + " @ " + game.homeTeam + " on " + CcyymmddDelimFormatter.print(date) + " ***")
  logger.info("**********************************************************")

  val gameId = game.homeTeam.toUpperCase + CcyymmddFormatter.print(date)

  val fileName = DataRoot + "gamedayPages/" + date.getYear + "/" + game.visitingTeam + "_" + game.homeTeam + "_" + CcyymmddFormatter.print(date) + "_box.html"

  if (new File(fileName).exists) {
    implicitlyWait(Span(10, Seconds))
    val caps = DesiredCapabilities.chrome
    caps.setCapability("chrome.switches", Array("--disable-javascript"))
    go to "file://" + fileName
  } else {
    implicitlyWait(Span(30, Seconds))
    go to game.gamedayUrl
  }
  if (!(new File(fileName)).exists) {
    val writer = new FileWriter(new File(fileName))
    writer.write(pageSource)
    writer.close
  }

  def playerFromMlbUrl(mlbUrl: WebElement): Player = {
    val url = mlbUrl.findElement(new ByTagName("a")).getAttribute("href")
    url match {
      case mlbIdExpression(urlString, mlbId) => realityballData.playerFromMlbId(mlbId, YearFormatter.print(date))
      case mlbIdExpression2(urlString, mlbId) => realityballData.playerFromMlbId(mlbId, YearFormatter.print(date))
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
            !playerClass.contains("ph") && !playerClass.contains("sub"),
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
    val linescores = {
      if (version_2_format) find(XPathQuery(linescoreType))
      else find(linescoreType)
    }
    (linescores match {
      case Some(x) => x.underlying match {
        case linescoresElement: RemoteWebElement => {
          linescoresElement.findElementsByTagName("tr") match {
            case batters: java.util.List[WebElement] => batters.foldLeft(List.empty[BatterLinescore])((x, y) => batterLinescore(x, y))
            case _                                   => throw new IllegalStateException("No linescore entries found")
          }
        }
      }
      case _ => List.empty[BatterLinescore]
    }) reverse
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
    val linescores = {
      if (version_2_format) find(XPathQuery(linescoreType))
      else find(linescoreType)
    }
    (linescores match {
      case Some(x) => x.underlying match {
        case linescoresElement: RemoteWebElement => {
          linescoresElement.findElementsByTagName("tr") match {
            case pitchers: java.util.List[WebElement] => pitchers.foldLeft(List.empty[PitcherLinescore])((x, y) => pitcherLinescore(x, y))
            case _ => throw new IllegalStateException("No linescore entries found")
          }
        }
      }
      case _ => List.empty[PitcherLinescore]
    }) reverse
  }

  val gameInfo: GameInfo = {
    val info = {
      if (version_2_format) find(XPathQuery("""//*[@id="boxscore"]/div/div/section[1]/div"""))
      else find("game-info-container")
    }
    info match {
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
          case _ => GameInfo("", "", "", "", "", "", "", "", "", "", "", "")
        }
      }
      case _ => GameInfo("", "", "", "", "", "", "", "", "", "", "", "")
    }
  }

  val pitchingResults: List[(WebElement, WebElement)] = {
    find(XPathQuery("""//*[@id="text_matchup"]/div[4]""")) match {
      case Some(x) => x.underlying match {
        case y: RemoteWebElement => y.findElementsByTagName("div").toList.zip(y.findElementsByTagName("a").toList)
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

  def pitcherForTypeNew(resultType: String): Player = {
    val url = find(XPathQuery("""//*[@id="text_matchup"]/div[4]/div[contains(@class, '""" + resultType + """')]""")) match {
      case Some(x) => x.underlying match {
        case x: RemoteWebElement => x
        case _ => null
      }
      case _ => null
    }
    if (url != null) playerFromMlbUrl(url)
    else null
  }

  //*[@id="text_matchup"]/div[3]/div[1]/a

  val winningPitcher = {
    if (version_2_format) pitcherForTypeNew("winner")
    else pitcherForType("W")
  }
  val losingPitcher = {
    if (version_2_format) pitcherForTypeNew("loser")
    else pitcherForType("L")
  }
  val savingPitcher = {
    if (version_2_format) pitcherForTypeNew("save")
    else pitcherForType("SV")
  }

  val awayBatterLinescores = {
    if (version_2_format) batterLinescores("""//*[@id="boxscore"]/div/div/div[1]/section[1]/section/table/tbody""")
    else batterLinescores("away-team-batter")
  }
  val homeBatterLinescores = {
    if (version_2_format)  batterLinescores("""//*[@id="boxscore"]/div/div/div[1]/section[2]/section/table/tbody""")
    else batterLinescores("home-team-batter")
  }
  val awayPitcherLinescores = {
    if (version_2_format) pitcherLinescores("""//*[@id="boxscore"]/div/div/div[2]/section[1]/section/table/tbody""")
    else pitcherLinescores("away-team-pitcher")
  }
  val homePitcherLinescores = {
    if (version_2_format) pitcherLinescores("""//*[@id="boxscore"]/div/div/div[2]/section[2]/section/table/tbody""")
    else pitcherLinescores("home-team-pitcher")
  }
  val awayStartingLineup = awayBatterLinescores.filter { _.starter }
  val homeStartingLineup = homeBatterLinescores.filter { _.starter }

  quit
}
