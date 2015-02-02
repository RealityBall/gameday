package org.bustos.realityball

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

class CoversLines(team: Team, year: String, runningGames: Map[String, List[GameOdds]]) extends Chrome {

  implicitlyWait(Span(20, Seconds))

  val dateExpression: Regex = ".*([0-9][0-9])/([0-9][0-9])/([0-9][0-9]).*".r
  val visitorExpression: Regex = "@(.*)".r
  val moneyLineExpression: Regex = "([LW])(.*)".r
  val oddsExpression: Regex = ".*([OUP]) (.*) (.*)".r

  val logger = LoggerFactory.getLogger(getClass)
  logger.info("********************************")
  logger.info("*** Retrieving odds for " + team.mlbComName + " for year " + year)
  logger.info("********************************")

  val host = "http://www.covers.com/"
  go to host + "pageLoader/pageLoader.aspx?page=/data/mlb/teams/pastresults/" + year + "/team" + team.coversComId + ".html"
  Thread sleep 5

  val retrosheetIdFromCovers = {
    val realityballData = new RealityballData
    val mappings = realityballData.teams(year).map(x => (x.coversComName -> x.mnemonic)).toMap
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
                    val moneyLine = cleanString(columnList(5).getAttribute("textContent")) match {
                      case moneyLineExpression(wl, lineValue) => lineValue.toInt
                      case _                                  => 0
                    }
                    val odds = columnList(6).getAttribute("textContent").replaceAll("\n", "") match {
                      case oddsExpression(oup, overUnder, odds) => if (odds == "-") (overUnder.toDouble, 0) else (overUnder.toDouble, odds.toInt)
                      case _                                    => (0.0, 0)
                    }
                    if (date == "20130527" && (visitor == "ARI" || team.mnemonic == "ARI")) {
                      println("")
                    }
                    if (visitor != "") {
                      val gameId = retrosheetIdFromCovers(visitor) + date
                      if (gameMap.contains(gameId)) {
                        if (gameMap(gameId).size == 2 && gameMap(gameId)(1).visitorML == 0) {
                          // Update `visitorML'
                          gameMap(gameId)(1).visitorML = moneyLine; gameOddsFromRows(gameMap, gameList.tail)
                        } else if (gameMap(gameId).size == 2 && gameMap(gameId)(0).visitorML == 0) {
                          // Update `visitorML'
                          gameMap(gameId)(0).visitorML = moneyLine; gameOddsFromRows(gameMap, gameList.tail)
                        } else {
                          // Probable Double Header
                          val secondOfDoubleHeader = gameMap(gameId)(0)
                          secondOfDoubleHeader.id = gameId + "2"
                          val firstOfDoubleHeader = GameOdds(gameId + "1", moneyLine, 0, odds._1, odds._2)
                          gameOddsFromRows(gameMap + (gameId -> List(firstOfDoubleHeader, secondOfDoubleHeader)), gameList.tail)
                        }
                      } else gameOddsFromRows(gameMap + (gameId -> List(GameOdds(gameId + "0", moneyLine, 0, odds._1, odds._2))), gameList.tail)
                    } else {
                      val gameId = team.mnemonic + date
                      if (gameMap.contains(gameId)) {
                        if (gameMap(gameId).size == 2 && gameMap(gameId)(1).homeML == 0) {
                          // Update `homerML'
                          gameMap(gameId)(1).homeML = moneyLine; gameOddsFromRows(gameMap, gameList.tail)
                        } else if (gameMap(gameId).size == 2 && gameMap(gameId)(0).homeML == 0) {
                          // Update `homeML'
                          gameMap(gameId)(0).homeML = moneyLine; gameOddsFromRows(gameMap, gameList.tail)
                        } else {
                          // Probable Double Header
                          val secondOfDoubleHeader = gameMap(gameId)(0)
                          secondOfDoubleHeader.id = gameId + "2"
                          val firstOfDoubleHeader = GameOdds(gameId + "1", 0, moneyLine, odds._1, odds._2)
                          gameOddsFromRows(gameMap + (gameId -> List(firstOfDoubleHeader, secondOfDoubleHeader)), gameList.tail)
                        }
                      } else gameOddsFromRows(gameMap + (gameId -> List(GameOdds(gameId + "0", 0, moneyLine, odds._1, odds._2))), gameList.tail)
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

  quit
}
