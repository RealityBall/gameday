package org.bustos.realityball

import org.scalatest._
import selenium._
import org.scalatest.time.{Span, Seconds}
import org.openqa.selenium.support.ui.{WebDriverWait, ExpectedCondition}
import org.openqa.selenium._
import htmlunit._
import scala.util.matching.Regex
import org.slf4j.LoggerFactory
import RealityballRecords._
import RealityballConfig._

object MlbSchedule {

  import java.io.File
  import scala.io.Source
  
  val retrosheetIdFromName = { 
    val file = new File(DataRoot + "teamMetaData.csv")
    Source.fromFile(file).getLines.map { line => 
      val data = line.split(',')
      data(4) -> data(0)
    }.toMap
  }

  val siteIdFromName = { 
    val file = new File(DataRoot + "teamMetaData.csv")
    Source.fromFile(file).getLines.map { line => 
      val data = line.split(',')
      data(0) -> data(1)
    }.toMap
  }

}

class MlbSchedule(teamData: TeamMetaData, year: String) extends WebBrowser {

  import MlbSchedule._
  
  implicit val webDriver: WebDriver = new HtmlUnitDriver(true)
  implicitlyWait(Span(10, Seconds))

  val logger =  LoggerFactory.getLogger(getClass)
  logger.info ("********************************")
  logger.info ("*** Retrieving team schedule for " + teamData.mlbComName + " for year " + year)
  logger.info ("********************************")
  
  val dateExpression: Regex = "(.*), (.*)/(.*)".r
  val awayExpression: Regex = "at (.*)".r
  val amTimeExpression: Regex = "(.*):(.*)a".r
  val pmTimeExpression: Regex = "(.*):(.*)p".r
  
  val host = GamedayURL
  go to host + "sortable.jsp?c_id=" + teamData.mlbComId + "&year=" + year
  Thread sleep 5
  
  val forecast = new HourlyWeatherForecast(teamData.zipCode)
  
  val games = (0 to 162) map { gameNo => 
    val rowName = "future_r" + gameNo
    find(rowName + "c1") match {
      case Some(element) => { element.text match {
          case awayExpression(where) => logger.info("Away game at " + where + ", do not create game")
          case visitingTeam: String => {   
            val date = find(rowName + "c0").get.text match {
              case dateExpression(dow, month, day) => year + (if (month.toInt < 10) "0" else "") + month + (if (day.toInt < 10) "0" else "") + day
              case _ => ""
            }
            val gameId = teamData.retrosheetId + date + "0"
            val time = find(rowName + "c2").get.text match {
              case pmTimeExpression(hours, minutes) => (hours.toInt + 12).toString + ":" + minutes
              case amTimeExpression(hours, minutes) => (if (hours.toInt < 10) "0" else "") + hours + ":" + minutes
              case _ => "19:00"
            }
            val conditions = forecast.forecastConditions(date + " " + time)
            (Game(gameId, teamData.retrosheetId, retrosheetIdFromName(visitingTeam), siteIdFromName(teamData.retrosheetId), date, 0), 
                GameConditions(gameId, time, "", false, conditions.temp, "", {if (conditions.winddir == "") 0 else conditions.winddir.toInt}, "", "", conditions.sky))
          }
        }
      }
      case _ => 
    }
  }

  quit
}