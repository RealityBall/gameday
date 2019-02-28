package org.bustos.realityball

import org.bustos.realityball.common.RealityballConfig._
import org.bustos.realityball.common.RealityballRecords._
import org.bustos.realityball.common.{RealityballData, RealityballJsonProtocol}
import org.joda.time._
import org.slf4j.LoggerFactory
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration.Duration.Inf

object Gameday extends App with RealityballJsonProtocol {

  System.setProperty("webdriver.chrome.driver", "/Applications/development/chromedriver")

  val logger = LoggerFactory.getLogger("Gameday")
  val realityballData = new RealityballData

  def processGamedayDate(eventFileDate: DateTime) = {
    def processGame(game: Game) = {
      val gamedayUrl = new GamedayUrl(game)
      val box = new MlbBox(gamedayUrl.validGame)
      val plays = new MlbPlays(gamedayUrl.validGame)
      new RetrosheetFromGameday(box, plays)
      logger.info("")
    }
    new RetrosheetClean(eventFileDate)
    realityballData.games(eventFileDate).foreach { processGame(_) }
  }

  def processSchedules(year: String) = {
    Await.result(db.run(gamedayScheduleTable.filter({ x => x.id like "%" + year + "%" }).delete), Inf)
    realityballData.teams(year).map(team => {
      val schedule = new MlbSchedule(team, year)
      Await.result(db.run(gamedayScheduleTable ++= {schedule.pastGames ++ schedule.futureGames}), Inf)
    })
  }

  def processOdds(year: Int) = {
    var gameOdds = Map.empty[String, List[GameOdds]]
    Await.result(db.run(gameOddsTable.filter({ x => x.id like ("%" + year + "%") }).delete), Inf)
    realityballData.teams(year.toString).foreach(team => {
      gameOdds = (new CoversLines(team, year.toString, gameOdds)).games
    })
    gameOdds.foreach({ case (k, v) => v.foreach({ x => Await.result(db.run(gameOddsTable += x), Inf) }) })
  }

  def processInjuries(date: DateTime) = {
    val injuries = (new MlbInjuries).injuries
    Await.result(db.run(injuryReportTable.filter({ x => x.injuryReportDate === date }).delete), Inf)
    injuries.filter(_.mlbId != null).foreach({ x => Await.result(db.run(injuryReportTable += x), Inf) })
  }

  def processLineups(date: DateTime) = {
    var lineups = new FantasyAlarmLineups(date)
    Await.result(db.run(lineupsTable.filter({ x => x.date === date }).delete), Inf)
    lineups.lineups.foreach({ case (k, v) => v.foreach({ x => Await.result(db.run(lineupsTable += x), Inf) }) })
  }

  try {
    Await.result(db.run(lineupsTable.schema.drop), Inf)
    Await.result(db.run(gamedayScheduleTable.schema.drop), Inf)
    //Await.result(db.run(gameOddsTable.schema.drop), Inf)
    Await.result(db.run(injuryReportTable.schema.drop), Inf)
  } catch {
    case e: Exception =>
  }
  Await.result(db.run(lineupsTable.schema.create), Inf)
  Await.result(db.run(gamedayScheduleTable.schema.create), Inf)
  //Await.result(db.run(gameOddsTable.schema.create), Inf)
  Await.result(db.run(injuryReportTable.schema.create), Inf)

  //processInjuries(DateTime.now)
  //(2018 to 2019).foreach(processOdds(_))
  //processLineups(new DateTime(2015, 4, 6, 0, 0))
  val startDate = new DateTime(2016, 3, 1, 0, 0)
  (for (f <- 0 to 50) yield startDate.plusDays(f)).foreach(processGamedayDate(_))

  //(2010 to 2015).foreach(processOdds(_))
  processSchedules("2016")

  logger.info("Completed Processing")
}
