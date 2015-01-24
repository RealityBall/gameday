package org.bustos.realityball

import java.io.File
import scala.io.Source
import org.slf4j.LoggerFactory
import java.util.Date

import RealityballRecords._
import RealityballConfig._

object Gameday extends App {

  System.setProperty("webdriver.chrome.driver", "/Users/mauricio/Downloads/chromedriver");
  val file = new File(DataRoot + "teamMetaData.csv")

  val logger = LoggerFactory.getLogger(getClass)
  
  def teamSchedule(teamData: TeamMetaData): MlbSchedule = {    
    try {
      new MlbSchedule(teamData, "2015")
    } catch {
      case e: Exception => new MlbSchedule(teamData, "2015")
    }
  }
  
  def processGames = {
    logger.info("Updating plays...")
    val game = new MlbPlays(new Date, "det", "ari")    
  }
  
  def processSchedules = {
    logger.info("Updating schedules...")
    db.withSession { implicit session =>
      Source.fromFile(file).getLines.foreach { line => 
        if (!line.startsWith("retrosheetId")) {
          val data = line.split(',')
          teamSchedule(TeamMetaData(data(0), data(1), data(2), data(3), data(4), data(5))).games.map { newData =>
            //gamesTable += newData.
            //  println("")
          }
        }
      }
    }    
  }
 
  processGames
  processSchedules
}
