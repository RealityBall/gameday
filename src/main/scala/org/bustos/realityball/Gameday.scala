package org.bustos.realityball

import org.joda.time._
import org.joda.time.format._
import java.io.File
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

  def processGame = {
    logger.info("Updating plays...")
    val game = new MlbPlays(new DateTime(2014, 4, 23, 0, 0), "mia", "atl")
    logger.info("Updating box scores...")
    val box = new MlbBox(new DateTime(2014, 4, 23, 0, 0), "mia", "atl")
    val retrosheet = new RetrosheetFromGameday(box, game)
    logger.info("")
  }

  def processSchedules(year: String) = {
    logger.info("Updating schedules for " + year + "...")

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
    logger.info("Updating Odds for " + year + "...")

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
    logger.info("Updating injuries for...")

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

  //processInjuries
  //(2010 to 2014).foreach(processOdds(_))
  //processSchedules("2014")
  //processSchedules("2015")
  processGame
  logger.info("Completed Processing")
}
