package org.bustos.realityball

import java.io._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import org.bustos.realityball.common.RealityballConfig._

object RetrosheetFromGameday {
  val Info = "info,"
  val Start = "start,"
  val Play = "play,"
  val NL = "\n"
}

class RetrosheetClean(date: DateTime) {
  val fileName = DataRoot + "generatedData/" + date.getYear + "/" + CcyymmddFormatter.print(date) + ".eve"

  if (new File(fileName).exists) new File(fileName).delete
}

class RetrosheetFromGameday(box: MlbBox, plays: MlbPlays) {

  import RetrosheetFromGameday._

  val logger = LoggerFactory.getLogger(getClass)
  logger.info("**************************************************************************")
  logger.info("*** Creating Retrosheet files from Gameday for " + box.game.visitingTeam + " @ " + box.game.homeTeam + " on " + box.game.date + " ***")
  logger.info("**************************************************************************")

  val gameInfo = box.gameInfo

  if (gameInfo.id != "") {
    val fileName = DataRoot + "generatedData/" + box.date.getYear + "/" + CcyymmddFormatter.print(box.date) + ".eve"

    val writer = {
      if (new File(fileName).exists) new FileWriter(new File(fileName), true) else new FileWriter(new File(fileName))
    }

    writer.write("id," + box.gameId + "0" + NL)
    // Game Info
    writer.write(Info + "visteam," + box.game.visitingTeam.toUpperCase + NL)
    writer.write(Info + "hometeam," + box.game.homeTeam.toUpperCase + NL)
    writer.write(Info + "date," + CcyymmddSlashDelimFormatter.print(box.date) + NL)
    writer.write(Info + "site," + box.gameInfo.venue + NL)
    writer.write(Info + "timeofgame," + box.gameInfo.time + NL)
    writer.write(Info + "attendance," + box.gameInfo.attendance + NL)
    // Weather
    writer.write(Info + "temp," + box.gameInfo.temp + NL)
    writer.write(Info + "winddir," + box.gameInfo.windDir + NL)
    writer.write(Info + "windspeed," + box.gameInfo.windSpeed + NL)
    writer.write(Info + "sky," + box.gameInfo.sky + NL)
    // Pitching Results
    writer.write(Info + "wp,")
    if (box.winningPitcher != null) writer.write(box.winningPitcher.id)
    writer.write(NL)
    writer.write(Info + "lp,")
    if (box.losingPitcher != null) writer.write(box.losingPitcher.id)
    writer.write(NL)
    writer.write(Info + "save,")
    if (box.savingPitcher != null) writer.write(box.savingPitcher.id)
    writer.write(NL)
    // Starting Lineups
    val awayLineup = box.awayStartingLineup.foldLeft((1, false)) { (x, y) =>
      writer.write(Start + y.player.id + ",\"" + y.player.firstName + " " + y.player.lastName + "\",0," + x._1)
      if (y.player.id == box.awayPitcherLinescores.head.player.id) {
        writer.write(",1" + NL)
        (x._1 + 1, true)
      } else {
        writer.write(",0" + NL)
        (x._1 + 1, x._2 || false)
      }
    }
    if (!awayLineup._2) {
      val awayStarter = box.awayPitcherLinescores.head.player
      writer.write(Start + awayStarter.id + ",\"" + awayStarter.firstName + " " + awayStarter.lastName + "\",0,0,1" + NL)
    }
    val homeLineup = box.homeStartingLineup.foldLeft((1, false)) { (x, y) =>
      writer.write(Start + y.player.id + ",\"" + y.player.firstName + " " + y.player.lastName + "\",1," + x._1)
      if (y.player.id == box.homePitcherLinescores.head.player.id) {
        writer.write(",1" + NL)
        (x._1 + 1, true)
      } else {
        writer.write(",0" + NL)
        (x._1 + 1, x._2 || false)
      }
    }
    if (!homeLineup._2) {
      val homeStarter = box.homePitcherLinescores.head.player
      writer.write(Start + homeStarter.id + ",\"" + homeStarter.firstName + " " + homeStarter.lastName + "\",1,0,1" + NL)
    }
    // Plays, incl. substitutions
    plays.playStrings.foreach {
      writer.write(_)
    }
    // Summary Data: Earned Runs

    writer.close
  }

}
