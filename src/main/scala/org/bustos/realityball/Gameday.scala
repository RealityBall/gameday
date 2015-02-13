package org.bustos.realityball

import org.joda.time._
import org.joda.time.format._
import java.io._
import org.slf4j.LoggerFactory
import scala.io.Source
import scala.slick.driver.MySQLDriver.simple._
import scala.slick.jdbc.meta.MTable

import RealityballRecords._
import RealityballConfig._

object Gameday extends App {

  System.setProperty("webdriver.chrome.driver", "/Applications/development/chromedriver");

  val logger = LoggerFactory.getLogger(getClass)
  val realityballData = new RealityballData

  def processGamedayDate(eventFileDate: DateTime) = {
    def processGame(game: Game) = {
      val plays = new MlbPlays(game)
      val box = new MlbBox(game)
      val retrosheet = new RetrosheetFromGameday(box, plays)
      logger.info("")
    }
    val eventFileName = DataRoot + "generatedData/" + eventFileDate.getYear + "/" + CcyymmddFormatter.print(eventFileDate) + ".eve"
    new FileWriter(new File(eventFileName))
    //realityballData.games(eventFileDate).filter(_.visitingTeam == "COL").foreach { processGame(_) }
    realityballData.games(eventFileDate).foreach { processGame(_) }
  }

  def processSchedules(year: String) = {
    db.withSession { implicit session =>
      gamedayScheduleTable.filter({ x => x.date startsWith year }).delete
      realityballData.teams(year).foreach(team => {
        val schedule = new MlbSchedule(team, year)
        schedule.pastGames.map { game =>
          gamedayScheduleTable += game
        }
        schedule.futureGames.map { game =>
          gamedayScheduleTable += game
        }
      })
    }
  }

  def processOdds(year: Int) = {
    var gameOdds = Map.empty[String, List[GameOdds]]
    db.withSession { implicit session =>
      gameOddsTable.filter({ x => x.id like ("%" + year + "%") }).delete
      realityballData.teams(year.toString).foreach(team => {
        gameOdds = (new CoversLines(team, year.toString, gameOdds)).games
      })
      gameOdds.foreach({ case (k, v) => v.foreach({ gameOddsTable += _ }) })
    }
  }

  def processInjuries = {
    var injuries = (new MlbInjuries).injuries
    db.withSession { implicit session =>
      injuries.foreach({ injuryReportTable += _ })
    }
  }

  db.withSession { implicit session =>
    if (MTable.getTables("gamedaySchedule").list.isEmpty) {
      gamedayScheduleTable.ddl.create
    }
    if (MTable.getTables("gameOdds").list.isEmpty) {
      gameOddsTable.ddl.create
    }
    if (MTable.getTables("injuryReport").list.isEmpty) {
      injuryReportTable.ddl.create
    }
  }

  processInjuries
  //(2010 to 2014).foreach(processOdds(_))
  (2015 to 2015).foreach(processOdds(_))
  processSchedules("2014")
  processSchedules("2015")

  //processGamedayDate(new DateTime(2014, 4, 24, 0, 0))
  //processGamedayDate(new DateTime(2014, 5, 24, 0, 0))
  //processGamedayDate(new DateTime(2014, 6, 24, 0, 0))
  //processGamedayDate(new DateTime(2014, 7, 24, 0, 0))
  processGamedayDate(new DateTime(2014, 8, 24, 0, 0))
  processGamedayDate(new DateTime(2014, 4, 14, 0, 0))
  processGamedayDate(new DateTime(2014, 5, 14, 0, 0))
  processGamedayDate(new DateTime(2014, 6, 14, 0, 0))
  processGamedayDate(new DateTime(2014, 7, 14, 0, 0))
  processGamedayDate(new DateTime(2014, 8, 14, 0, 0))
  logger.info("Completed Processing")
}
