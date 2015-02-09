package org.bustos.realityball

import java.io._
import org.slf4j.LoggerFactory
import RealityballConfig._

object RetrosheetFromGameday {
  val Info = "info,"
  val Start = "start,"
  val Play = "play,"
  val NL = "\n"
}

class RetrosheetFromGameday(box: MlbBox, plays: MlbPlays) {

  import RetrosheetFromGameday._

  val logger = LoggerFactory.getLogger(getClass)
  logger.info("********************************")
  logger.info("*** Creating Retrosheet files from Gameday for " + box.game.visitingTeam + " @ " + box.game.homeTeam + " on " + box.game.date)
  logger.info("********************************")

  val fileName = DataRoot + "generatedData/" + box.date.getYear + "/" + CcyymmddFormatter.print(box.date) + ".eve"

  val writer = { if (new File(fileName).exists) new FileWriter(new File(fileName), true) else new FileWriter(new File(fileName)) }

  writer.write("id," + box.gameId + "0" + NL)
  // Game Info
  writer.write(Info + "visteam," + box.game.visitingTeam.toUpperCase + NL)
  writer.write(Info + "hometeam," + box.game.homeTeam.toUpperCase + NL)
  writer.write(Info + "site," + box.gameInfo.venue + NL)
  writer.write(Info + "timeofgame," + box.gameInfo.time + NL)
  writer.write(Info + "attendance," + box.gameInfo.attendance + NL)
  // Weather
  writer.write(Info + "temp," + box.gameInfo.temp + NL)
  writer.write(Info + "winddir," + box.gameInfo.windDir + NL)
  writer.write(Info + "windspeed," + box.gameInfo.windSpeed + NL)
  writer.write(Info + "sky," + box.gameInfo.sky + NL)
  // Pitching Results
  writer.write(Info + "wp,"); if (box.winningPitcher != null) writer.write(box.winningPitcher.id);
  writer.write(NL)
  writer.write(Info + "lp,"); if (box.losingPitcher != null) writer.write(box.losingPitcher.id);
  writer.write(NL)
  writer.write(Info + "save,"); if (box.savingPitcher != null) writer.write(box.savingPitcher.id);
  writer.write(NL)
  // Starting Lineups
  box.awayStartingLineup.foldLeft(1) { (x, y) => writer.write(Start + y.player.id + ",\"" + y.player.firstName + " " + y.player.lastName + "\",0," + x + ",0" + NL); x + 1 }
  box.homeStartingLineup.foldLeft(1) { (x, y) => writer.write(Start + y.player.id + ",\"" + y.player.firstName + " " + y.player.lastName + "\",1," + x + ",0" + NL); x + 1 }
  // Plays, incl. substitutions
  plays.playStrings.foreach { writer.write(_) }
  // Summary Data: Earned Runs

  writer.close

}
