package org.bustos.realityball

import java.io._
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
import RealityballRecords._
import RealityballConfig._

class CoversLines(team: Team, year: String, runningGames: Map[String, List[GameOdds]], past: Boolean) extends Chrome {

  implicitlyWait(Span(20, Seconds))

  val dateExpression: Regex = ".*([0-9][0-9])/([0-9][0-9])/([0-9][0-9]).*".r
  val visitorExpression: Regex = "@(.*)".r
  val moneyLineExpression: Regex = "([LW])(.*)".r
  val oddsExpression: Regex = ".*([OUP]) (.*) (.*)".r
  val futureOddsML: Regex = "(.*)/(.*)([ou])(.*)".r // e.g. 115/7o -112

  val logger = LoggerFactory.getLogger(getClass)
  val pastFuture = if (past) " (past)   " else " (future) "

  logger.info("*********************************************************")
  logger.info("*** Retrieving odds for " + team.mlbComName + " for year " + year + pastFuture + "***")
  logger.info("*********************************************************")

  val host = "http://www.covers.com/"

  val fileName = DataRoot + "coversPages/" + year + "/" + team.mnemonic + "_odds.html"

  if (year < "2015") {
    if (new File(fileName).exists) {
      val caps = DesiredCapabilities.chrome;
      caps.setCapability("chrome.switches", Array("--disable-javascript"));
      go to "file://" + fileName
    } else {
      go to host + "pageLoader/pageLoader.aspx?page=/data/mlb/teams/schedule/team" + team.coversComId + ".html"
    }
  } else {
    if (past) go to host + "pageLoader/pageLoader.aspx?page=/data/mlb/teams/pastresults/" + year + "/team" + team.coversComId + ".html"
    else go to host + "pageLoader/pageLoader.aspx?page=/data/mlb/teams/schedule/team" + team.coversComId + ".html"

  }
  Thread sleep 5

  val retrosheetIdFromCovers = {
    val realityballData = new RealityballData
    val mappings = {
      val lYear = if (year == "2015") "2014" else year
      if (past) realityballData.teams(lYear).map(x => (x.coversComName -> x.mnemonic)).toMap
      else realityballData.teams(lYear).map(x => (cleanString(x.coversComFullName) -> x.mnemonic)).toMap
    }
    if (year == "2010" || year == "2011") mappings + ("MIA" -> "FLO")
    else mappings
  }

  private def cleanString(string: String): String = {
    string.replaceAll("\n", "").replaceAll(" ", "").replaceAll("\t", "")
  }

  def gameOddsFromRows(gameMap: Map[String, List[GameOdds]], gameList: List[WebElement]): Map[String, List[GameOdds]] = {
    if (gameList.isEmpty) gameMap
    else {
      gameList.head match {
        case gameElement: RemoteWebElement => {
          gameElement.getAttribute("textContent") match {
            case gameEntry: String => {
              gameElement.findElementsByTagName("td") match {
                case columnList: java.util.List[WebElement] => {
                  if (columnList(0).getAttribute("textContent") == "Date") gameOddsFromRows(gameMap, gameList.tail)
                  else {
                    val date = cleanString(columnList(0).getAttribute("textContent")) match {
                      case dateExpression(month, day, year) => "20" + year + month + day
                    }
                    val visitor = cleanString(columnList(1).getAttribute("textContent")) match {
                      case visitorExpression(team) => team
                      case _                       => ""
                    }
                    val moneyLine =
                      if (past) {
                        cleanString(columnList(5).getAttribute("textContent")) match {
                          case moneyLineExpression(wl, lineValue) => lineValue.toInt
                          case _ => 0
                        }
                      } else {
                        cleanString(columnList(3).getAttribute("textContent")) match {
                          case futureOddsML(ml, overUnder, oup, overUnderML) => ml.toInt
                          case _ => 0
                        }
                      }

                    val odds =
                      if (past) {
                        columnList(6).getAttribute("textContent").replaceAll("\n", "") match {
                          case oddsExpression(oup, overUnder, odds) => if (odds == "-") (overUnder.toDouble, 0) else (overUnder.toDouble, odds.toInt)
                          case _                                    => (0.0, 0)
                        }
                      } else {
                        cleanString(columnList(3).getAttribute("textContent")) match {
                          case futureOddsML(ml, overUnder, oup, overUnderML) => if (overUnderML == "-") (overUnder.toDouble, 0) else (overUnder.toDouble, overUnderML.toInt)
                          case _ => (0.0, 0)
                        }
                      }
                    if (visitor != "") {
                      val gameId = retrosheetIdFromCovers(visitor) + date
                      if (gameMap.contains(gameId + past)) {
                        if (gameMap(gameId + past).size == 2 && gameMap(gameId + past)(1).visitorML == 0) {
                          // Update `visitorML' for second game of a double header
                          gameMap(gameId + past)(1).visitorML = moneyLine; gameOddsFromRows(gameMap, gameList.tail)
                        } else if (gameMap(gameId + past).size == 2 && gameMap(gameId + past)(0).visitorML == 0) {
                          // Update `visitorML' for first game of a double header
                          gameMap(gameId + past)(0).visitorML = moneyLine; gameOddsFromRows(gameMap, gameList.tail)
                        } else if (gameMap(gameId + past)(0).visitorML == 0) {
                          // Update `visitorML'
                          gameMap(gameId + past)(0).visitorML = moneyLine; gameOddsFromRows(gameMap, gameList.tail)
                        } else {
                          // Probable Double Header
                          val secondOfDoubleHeader = gameMap(gameId + past)(0)
                          secondOfDoubleHeader.id = gameId + past + "2"
                          val firstOfDoubleHeader = GameOdds(gameId + past + "1", moneyLine, 0, odds._1, odds._2)
                          gameOddsFromRows(gameMap + (gameId + past -> List(firstOfDoubleHeader, secondOfDoubleHeader)), gameList.tail)
                        }
                      } else gameOddsFromRows(gameMap + (gameId + past -> List(GameOdds(gameId + "0", moneyLine, 0, odds._1, odds._2))), gameList.tail)
                    } else {
                      val gameId = team.mnemonic + date
                      if (gameMap.contains(gameId + past)) {
                        if (gameMap(gameId + past).size == 2 && gameMap(gameId + past)(1).homeML == 0) {
                          // Update `homerML' for second game of a double header
                          gameMap(gameId + past)(1).homeML = moneyLine; gameOddsFromRows(gameMap, gameList.tail)
                        } else if (gameMap(gameId + past).size == 2 && gameMap(gameId + past)(0).homeML == 0) {
                          // Update `homeML' for first game of a double header
                          gameMap(gameId + past)(0).homeML = moneyLine; gameOddsFromRows(gameMap, gameList.tail)
                        } else if (gameMap(gameId + past)(0).homeML == 0) {
                          // Update `homeML'
                          gameMap(gameId + past)(0).homeML = moneyLine; gameOddsFromRows(gameMap, gameList.tail)
                        } else {
                          // Probable Double Header
                          val secondOfDoubleHeader = gameMap(gameId + past)(0)
                          secondOfDoubleHeader.id = gameId + past + "2"
                          val firstOfDoubleHeader = GameOdds(gameId + past + "1", 0, moneyLine, odds._1, odds._2)
                          gameOddsFromRows(gameMap + (gameId + past -> List(firstOfDoubleHeader, secondOfDoubleHeader)), gameList.tail)
                        }
                      } else gameOddsFromRows(gameMap + (gameId + past -> List(GameOdds(gameId + "0", 0, moneyLine, odds._1, odds._2))), gameList.tail)
                    }
                  }
                }
                case _ => gameOddsFromRows(gameMap, gameList.tail)
              }
            }
            case _ => gameOddsFromRows(gameMap, gameList.tail)
          }
        }
      }
    }
  }

  val games: Map[String, List[GameOdds]] = {
    find("LeftCol-wss") match {
      case Some(x) => x.underlying match {
        case remoteElement: RemoteWebElement => remoteElement.findElementsByTagName("tr") match {
          case gameList: java.util.List[WebElement] => gameOddsFromRows(runningGames, gameList.toList)
          case _                                    => runningGames
        }
        case _ => throw new IllegalStateException("Could not decode odds tables")
      }
      case _ => throw new IllegalStateException("Could not find odds tables")
    }
  }

  if (year < "2015") {
    if (!(new File(fileName)).exists) {
      val writer = new FileWriter(new File(fileName))
      writer.write(pageSource)
      writer.close
    }
  }

  quit
}
