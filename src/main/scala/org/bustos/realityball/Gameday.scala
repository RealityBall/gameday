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

  def teamSchedule(team: Team, year: String): MlbSchedule = {
    try {
      new MlbSchedule(team, year)
    } catch {
      case e: Exception => new MlbSchedule(team, year)
    }
  }

  def processSchedules(year: String) = {
    logger.info("Updating schedules for " + year + "...")

    db.withSession { implicit session =>
      gamedayScheduleTable.filter({ x => x.date startsWith year }).delete
      realityballData.teams(year).foreach(team => {
        val schedule = teamSchedule(team, year)
        schedule.pastGames.map { game =>
          gamedayScheduleTable += game
        }
        schedule.futureGames.map { game =>
          gamedayScheduleTable += game
        }
      })
    }
  }

  db.withSession { implicit session =>
    if (MTable.getTables("gamedaySchedule").list.isEmpty) {
      gamedayScheduleTable.ddl.drop
      //gamedayScheduleTable.ddl.create
    }
  }
  //processSchedules("2014")
  //processSchedules("2015")
  processBoxScores
  processGamePlays
}
