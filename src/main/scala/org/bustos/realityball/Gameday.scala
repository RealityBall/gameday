package org.bustos.realityball

import java.io.File
import java.util.Date
import org.slf4j.LoggerFactory
import scala.io.Source
import scala.slick.driver.MySQLDriver.simple._
import scala.slick.jdbc.meta.MTable

import RealityballRecords._
import RealityballConfig._

object Gameday extends App {

  System.setProperty("webdriver.chrome.driver", "/Users/mauricio/Downloads/chromedriver");

  val logger = LoggerFactory.getLogger(getClass)
  val realityballData = new RealityballData

  def processGamePlays = {
    logger.info("Updating plays...")
    val game = new MlbPlays(new Date(114, 3, 22), "mia", "atl")
  }

  def processBoxScores = {
    logger.info("Updating box scores...")
    val box = new MlbBox(new Date(114, 3, 22), "mia", "atl")
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

  db.withSession { implicit session =>
    if (MTable.getTables("gamedaySchedule").list.isEmpty) {
      //gamedayScheduleTable.ddl.drop
      gamedayScheduleTable.ddl.create
    }
    if (MTable.getTables("gameOdds").list.isEmpty) {
      //gamedayScheduleTable.ddl.drop
      gameOddsTable.ddl.create
    }
  }

  (2013 to 2014).foreach(processOdds(_))
  //processSchedules("2014")
  //processSchedules("2015")
  processBoxScores
  processGamePlays
}
