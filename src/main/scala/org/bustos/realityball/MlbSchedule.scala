package org.bustos.realityball

import java.io._
import org.joda.time.DateTime
import org.scalatest._
import selenium._
import org.scalatest.time.{ Span, Seconds }
import org.openqa.selenium.support.ui.{ WebDriverWait, ExpectedCondition }
import org.openqa.selenium._
import htmlunit._
import remote._
import scala.util.matching.Regex
import scala.collection.JavaConversions._
import org.slf4j.LoggerFactory
import org.bustos.realityball.common.RealityballRecords._
import org.bustos.realityball.common.RealityballConfig._
import org.bustos.realityball.common.RealityballData
import org.bustos.realityball.common.Weather

object MlbSchedule {

  val futureGame = "future"
  val pastGame = "past"

  val retrosheetIdFromName = {
    val realityballData = new RealityballData
    realityballData.teams("2014").map(x => (x.mlbComName -> x.mnemonic)).toMap
  }

  val siteIdFromMnemonic = {
    val realityballData = new RealityballData
    realityballData.teams("2014").map(x => (x.mnemonic -> x.site)).toMap
  }

}

class MlbSchedule(team: Team, year: String) extends Chrome {

  import MlbSchedule._

  implicitlyWait(Span(20, Seconds))

  val logger = LoggerFactory.getLogger(getClass)
  logger.info("********************************")
  logger.info("*** Retrieving team schedule for " + team.mlbComName + " for year " + year)
  logger.info("********************************")

  val dateExpression: Regex = "(.*), (.*)/(.*)".r
  val awayExpression: Regex = ".*at (.*).*".r
  val amTimeExpression: Regex = "(.*):(.*)a".r
  val pmTimeExpression: Regex = "(.*):(.*)p".r

  val fileName = DataRoot + "gamedayPages/" + year + "/" + team.mnemonic + "_schedule.html"

  if (new File(fileName).exists) {
    val caps = DesiredCapabilities.chrome;
    caps.setCapability("chrome.switches", Array("--disable-javascript"));

    go to "file://" + fileName
  } else {
    val host = GamedayURL
    go to host + "schedule/sortable.jsp?c_id=" + team.mlbComId + "&year=" + year
  }

  Thread sleep 5

  val weather = new Weather(team.zipCode)

  def conditionsForDate(gameType: String, date: DateTime): GameConditions = {
    //if (gameType == futureGame) weather.forecastConditions(date)
    //else weather.historicalConditions(date)
    GameConditions("", new DateTime, "", false, 0, "", 0, "", "", "")
  }

  def scheduleFromRow(gameType: String, gameElement: RemoteWebElement): GamedaySchedule = {
    gameElement.getAttribute("textContent") match {
      case awayExpression(where) => GamedaySchedule("", "", "", "", new DateTime, 0, "", "", "", "", "", "", 0, "", 0, "", "")
      case homeGame: String => {
        val id = gameElement.getAttribute("id")
        val time = {
          if (gameType == futureGame) {
            find(id + "c2").get.text match {
              case pmTimeExpression(hours, minutes) => if (hours.toInt < 12) (hours.toInt + 12).toString + ":" + minutes else hours + ":" + minutes
              case amTimeExpression(hours, minutes) => (if (hours.toInt < 10) "0" else "") + hours + ":" + minutes
              case _                                => "19:00"
            }
          } else "19:00"
        }
        val date = find(id + "c0").get.text match {
          case dateExpression(dow, month, day) => {
            CcyymmddTimeFormatter.parseDateTime(year + (if (month.toInt < 10) "0" else "") + month + (if (day.toInt < 10) "0" else "") + day + " " + time)
          }
          case _                               => new DateTime
        }
        val visitingTeam = find(id + "c1").get.text
        val gameId = team.mnemonic + date + "0"
        val result = if (gameType == pastGame) find(id + "c2").get.text else ""
        val record = if (gameType == pastGame) find(id + "c3").get.text else ""
        val winningPitcher = if (gameType == pastGame) find(id + "c4").get.text else ""
        val losingPitcher = if (gameType == pastGame) find(id + "c5").get.text else ""
        val conditions = conditionsForDate(gameType, date)
        GamedaySchedule(gameId, team.mnemonic, retrosheetIdFromName(visitingTeam), siteIdFromMnemonic(team.mnemonic), date, 0,
          result, winningPitcher, losingPitcher, record, "", "",
          conditions.temp, "", { if (conditions.winddir == "") 0 else conditions.winddir.toInt }, "", conditions.sky)
      }
    }
  }

  def games(gameType: String) = (find(gameType) match {
    case Some(x) => x.underlying match {
      case remoteElement: RemoteWebElement => remoteElement.findElementsByTagName("tr") match {
        case gameList: java.util.List[WebElement] => {
          gameList.toList.map { game =>
            if (game.isDisplayed && game.getAttribute("id") != "") {
              game match {
                case gameElement: RemoteWebElement => scheduleFromRow(gameType, gameElement)
                case _                             => GamedaySchedule("", "", "", "", new DateTime, 0, "", "", "", "", "", "", 0, "", 0, "", "")
              }
            } else GamedaySchedule("", "", "", "", new DateTime, 0, "", "", "", "", "", "", 0, "", 0, "", "")
          }
        }
        case _ => List(GamedaySchedule("", "", "", "", new DateTime, 0, "", "", "", "", "", "", 0, "", 0, "", ""))
      }
    }
    case x => List(GamedaySchedule("", "", "", "", new DateTime, 0, "", "", "", "", "", "", 0, "", 0, "", ""))
  }) filter { _.id != "" }

  def findDoubleHeaders(games: List[GamedaySchedule]): List[GamedaySchedule] = {
    if (games.isEmpty || games.tail.isEmpty) games
    else if (games.head.id == games.tail.head.id) {
      games.head.id = games.head.id.substring(0, 11) + (games.head.id.substring(11, 12).toInt + 1).toString
      games.tail.head.id = games.tail.head.id.substring(0, 11) + (games.tail.head.id.substring(11, 12).toInt + 2).toString
      games.head :: findDoubleHeaders(games.tail)
    } else games.head :: findDoubleHeaders(games.tail)
  }

  val pastGames = findDoubleHeaders(games(pastGame))
  val futureGames = findDoubleHeaders(games(futureGame))

  if (!(new File(fileName)).exists) {
    val writer = new FileWriter(new File(fileName))
    writer.write(pageSource)
    writer.close
  }

  quit
}
