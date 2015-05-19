package org.bustos.realityball

import org.joda.time._
import org.joda.time.format._
import java.io._
import org.slf4j.LoggerFactory
import scala.io.Source
import scala.slick.driver.MySQLDriver.simple._
import scala.slick.jdbc.meta.MTable

import org.bustos.realityball.common.RealityballRecords._
import org.bustos.realityball.common.RealityballConfig._
import org.bustos.realityball.common.RealityballData

object Gameday extends App {

  System.setProperty("webdriver.chrome.driver", "/Applications/development/chromedriver");

  val logger = LoggerFactory.getLogger(getClass)
  val realityballData = new RealityballData

  def processGamedayDate(eventFileDate: DateTime) = {
    def processGame(game: Game) = {
      val box = new MlbBox(game)
      val plays = new MlbPlays(game)
      val retrosheet = new RetrosheetFromGameday(box, plays)
      logger.info("")
    }
    new RetrosheetClean(eventFileDate)
    realityballData.games(eventFileDate).foreach { processGame(_) }
  }

  def processSchedules(year: String) = {
    db.withSession { implicit session =>
      gamedayScheduleTable.filter({ x => x.date startsWith year }).delete
      realityballData.teams(year).foreach(team => {
        val schedule = new MlbSchedule(team, year)
        schedule.pastGames.foreach { game =>
          gamedayScheduleTable += game
        }
        schedule.futureGames.foreach { game =>
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
        gameOdds = (new CoversLines(team, year.toString, gameOdds, true)).games
        gameOdds = (new CoversLines(team, year.toString, gameOdds, false)).games
      })
      gameOdds.foreach({ case (k, v) => v.foreach({ gameOddsTable += _ }) })
    }
  }

  def processInjuries(date: DateTime) = {
    var injuries = (new MlbInjuries).injuries
    db.withSession { implicit session =>
      injuryReportTable.filter({ x => x.injuryReportDate === CcyymmddFormatter.print(date) }).delete
      injuries.foreach({ injuryReportTable += _ })
    }
  }

  def processLineups(date: DateTime) = {
    var lineups = new FantasyAlarmLineups(date)
    db.withSession { implicit session =>
      lineupsTable.filter({ x => x.date === CcyymmddFormatter.print(date) }).delete
      lineups.lineups.foreach({ case (k, v) => v.foreach(lineupsTable += _) })
    }
  }

  db.withSession { implicit session =>
    if (MTable.getTables("lineups").list.isEmpty) {
      lineupsTable.ddl.create
    }
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

  processInjuries(DateTime.now)
  //processOdds(2015)
  //processLineups(new DateTime(2015, 4, 6, 0, 0))
  val startDate = new DateTime(2015, 5, 1, 0, 0)
  (for (f <- 0 to 20) yield startDate.plusDays(f)).foreach(processGamedayDate(_))

  //(2010 to 2014).foreach(processOdds(_))
  //processSchedules("2014")
  //processSchedules("2015")

  logger.info("Completed Processing")
}
