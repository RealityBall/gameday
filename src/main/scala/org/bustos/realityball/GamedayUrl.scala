package org.bustos.realityball

import org.bustos.realityball.common.RealityballConfig._
import org.bustos.realityball.common.RealityballRecords.Game
import org.bustos.realityball.common.RealityballRecords._
import org.scalatest.selenium._
import slick.jdbc.MySQLProfile.api._
import org.scalatest.time.{Seconds, Span}
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration.Inf

class GamedayUrl(val game: Game) extends Chrome {

  val logger = LoggerFactory.getLogger(getClass)

  logger.info("***************************************************")
  logger.info("*** Retrieving URL for " + game.visitingTeam + " @ " + game.homeTeam + " on " + CcyymmddDelimFormatter.print(game.date) + " ***")
  logger.info("***************************************************")

  val date = game.date
  val gameId = game.homeTeam.toUpperCase + CcyymmddFormatter.print(date)

  val url =
    if (game.gamedayUrl == "") {
      implicitlyWait(Span(30, Seconds))
      val host = GamedayURL
      go to host + "mlb/gameday/index.jsp?gid=" + CcyymmddDelimFormatter.print(date) + "_" + game.visitingTeam.toLowerCase + "mlb_" + game.homeTeam.toLowerCase + "mlb_1&mode=box"
      Await.result(db.run(gamesTable.filter(_.id === game.id).map((_.gamedayUrl)).update(currentUrl)), Inf)
      currentUrl
    } else game.gamedayUrl

  val validGame = Game(game.id, game.homeTeam, game.visitingTeam, game.site, game.date, game.number, game.startingHomePitcher, game.startingVisitingPitcher, url)

}
