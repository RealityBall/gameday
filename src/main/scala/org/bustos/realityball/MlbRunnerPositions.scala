package org.bustos.realityball

import org.bustos.realityball.common.RealityballRecords._
import org.bustos.realityball.common.RealityballData
import org.slf4j.LoggerFactory

object MlbRunnerPositions {
  val nameExpression = """(A.  J.  )?(C.  J.  )?(B.  J.  )?(L.  J.  )?(T.  J.  )?(J.  P.  )?(J.  D.  )?(R.  J.  )?(Juan Carlos)?(John Ryan)?(J.  )?(.*?) (.*)""".r
  val outAt2ndExpression = """.*[\.,:]  *(.*) out at 2nd.*""".r
  val outAt3rdExpression = """.*[\.,:]  *(.*) out at 3rd.*""".r
  val outAtHomeExpression = """.*[\.,:]  *(.*) out at home.*""".r
  val to1stExpression = """.*[\.,:]  *(.*) to 1st.*""".r
  val to2ndExpression = """.*[\.,:]  *(.*) to 2nd.*""".r
  val to3rdExpression = """.*[\.,:]  *(.*) to 3rd.*""".r
  val poAt1stExpression = """.* picks off (.*) at 1st.*""".r
  val poAt2ndExpression = """.* picks off (.*) at 2nd.*""".r
  val poAt3rdExpression = """.* picks off (.*) at 3rd.*""".r
  val advancesTo1stExpression = """.*[\.,:]  *(.*) advances to 1st.*""".r
  val advancesTo2ndExpression = """.*[\.,:]  *(.*) advances to 2nd.*""".r
  val advancesTo3rdExpression = """.*[\.,:]  *(.*) advances to 3rd.*""".r
  val scoresExpression = """.*[\.,:]  *(.*) scores.*""".r
  val twoScoresExpression = """.*[\.,:]  *(.*) scores\.  *(.*) scores.*""".r
  val threeScoresExpression = """.*[\.,:]  *(.*) scores\.  *(.*) scores\.  *(.*) scores.*""".r
}

class MlbRunnerPositions {

  import MlbRunnerPositions._

  val realityballData = new RealityballData
  val logger = LoggerFactory.getLogger(getClass)

  var positions = Map.empty[Player, String]

  def lastName(name: String): String = {
    name match {
      case nameExpression(aj, cj, bj, lj, tj, jp, jc, jd, rj, jr, jj, firstName, lastName) => lastName
      case _ => ""
    }
  }

  def playerFromMlbId(mlbId: String, year: String): Player = {
    realityballData.playerFromMlbId(mlbId, year)
  }

  def playerFromString(name: String, team: String, year: String): Player = {
    val trimmedName = if (name == "pollock") "a pollock" else name.replace(" Jr.", "").trim
    trimmedName match {
      case nameExpression(aj, cj, bj, lj, tj, jp, jc, jd, rj, jr, jj, firstName, lastName) => {
        val fName: String = {
          if (aj != null) aj
          else if (cj != null) cj
          else if (bj != null) bj
          else if (lj != null) lj
          else if (tj != null) tj
          else if (jp != null) jp
          else if (jc != null) jc
          else if (jd != null) jd
          else if (jr != null) jr
          else if (jj != null) jj
          else if (rj != null) rj
          else firstName
        }
        try {
          realityballData.playerFromName(fName(0).toString, lastName.trim, year, team)
        } catch {
          case e: Exception => {
            logger.warn("Initial attempt to find player failed: " + trimmedName + " (" + team + ")")
            // These are one off bugs from mlb.com where they confuse the first name on an advancement.
            if (trimmedName == "j   ellis") realityballData.playerFromName("A", "Ellis", year, team)
            else if (trimmedName == "b   shuck") realityballData.playerFromName("J", "Shuck", year, team)
            else if (trimmedName == "j  b   shuck") realityballData.playerFromName("J", "Shuck", year, team)
            else if (trimmedName == "j  a   happ") realityballData.playerFromName("J", "Happ", year, team)
            else if (trimmedName == "j   pollock") realityballData.playerFromName("A", "Pollock", year, team)
            else if (trimmedName == "j   pierzynski") realityballData.playerFromName("A", "Pierzynski", year, team)
            else if (trimmedName == "jackie bradley    brock holt") realityballData.playerFromName("B", "Holt", year, team)
            else if (trimmedName == "David Carpenter" && (team == "NYA" || team == "WAS") && year == "2015") realityballData.playerFromRetrosheetId("502304", year)  // Traded from ATL to NYA then to WAS
            else if (trimmedName == "Sugar Ray Marimon") realityballData.playerFromRetrosheetId("516970", year)  // New player with hard parsing
            else realityballData.playerFromName(fName.trim, lastName.trim, year, team)
          }
        }
      }
      case _ => {
        // These are one off bugs from mlb.com where they omit the first name on an advancement.
        if (name == "ellis") realityballData.playerFromName("A", "Ellis", year, team)
        else if (name == "wilson") realityballData.playerFromName("C", "Wilson", year, team)
        else if (name == "cron") realityballData.playerFromName("C", "Cron", year, team)
        else if (name == "hardy") realityballData.playerFromName("J", "Hardy", year, team)
        else if (name == "realmuto") realityballData.playerFromName("J", "Realmuto", year, team)
        else if (name == "pierzynski") realityballData.playerFromName("A", "Pierzynski", year, team)
        else {
          logger.warn("Could not decode name: " + trimmedName)
          null
        }
      }
    }
  }

  def pinchRunner(in: String, out: String, team: String, year: String) = {
    val newRunner = playerFromString(in, team, year)
    val oldRunner = playerFromString(out, team, year)
    positions += (newRunner -> positions(oldRunner))
    positions -= oldRunner
  }

  def advancementString(play: String, batter: Player, team: String, year: String): String = {
    var advancements = "."
    if (play.contains("single")) positions += (batter -> "1")
    if (play.contains("walk")) positions += (batter -> "1")
    if (play.contains("double")) positions += (batter -> "2")
    if (play.contains("triple")) positions += (batter -> "3")
    if (play.contains("hit by pitch")) { positions += (batter -> "1"); advancements = advancements + "B-1;" }
    if (play.contains("hits a sacrifice bunt")) { positions += (batter -> "1"); advancements = advancements + "B-1;" }
    if (play.contains("reaches on an interference error")) { positions += (batter -> "1"); advancements = advancements + "B-1;" }
    if (play.contains("reaches on a fielding error")) { positions += (batter -> "1"); advancements = advancements + "B-1;" }
    if (play.contains("reaches on a fielder's choice")) { positions += (batter -> "1"); advancements = advancements + "B-1;" }
    if (play.contains("reaches on a throwing error")) { positions += (batter -> "1"); advancements = advancements + "B-1;" }
    if (play.contains("reaches on a force attempt")) { positions += (batter -> "1"); advancements = advancements + "B-1;" }
    if (play.contains("reaches on a missed catch error")) { positions += (batter -> "1"); advancements = advancements + "B-1;" }
    if (play.contains("advances to 2nd, on a throwing error")) { positions += (batter -> "2"); advancements = advancements + "B-2;" }
    if (play.contains("advances to 3rd, on a throwing error")) { positions += (batter -> "3"); advancements = advancements + "B-3;" }
    play match {
      case poAt1stExpression(name) => positions -= playerFromString(name, team, year)
      case _                       =>
    }
    play match {
      case poAt2ndExpression(name) => positions -= playerFromString(name, team, year)
      case _                       =>
    }
    play match {
      case poAt3rdExpression(name) => positions -= playerFromString(name, team, year)
      case _                       =>
    }
    play match {
      case advancesTo1stExpression(name) =>
        val runner = playerFromString(name, team, year)
        if (runner == null || (batter.id == runner.id && !positions.contains(runner))) positions += (runner -> "1") // Reached first on a throwing error
        advancements = advancements + positions(runner) + "-1;"
        positions -= runner
        positions += (runner -> "1")
      case to1stExpression(name) =>
        positions += (playerFromString(name, team, year) -> "1"); advancements = advancements + "B-1;"
      case _ =>
    }
    play match {
      case advancesTo2ndExpression(name) => {
        val runner = playerFromString(name, team, year)
        if (runner == null || (batter.id == runner.id && !positions.contains(runner))) positions += (runner -> "1") // Reached first on a throwing error
        advancements = advancements + positions(runner) + "-2;"
        positions -= runner
        positions += (runner -> "2")
      }
      case to2ndExpression(name) => {
        val runner = playerFromString(name, team, year)
        advancements = advancements + positions(runner) + "-2;"
        positions -= runner
        positions += (runner -> "2")
      }
      case _ =>
    }
    play match {
      case advancesTo3rdExpression(name) => {
        val runner = playerFromString(name, team, year)
        advancements = advancements + positions(runner) + "-3;"
        positions -= runner
        positions += (runner -> "3")
      }
      case to3rdExpression(name) => {
        val runner = playerFromString(name, team, year)
        advancements = advancements + positions(runner) + "-3;"
        positions -= runner
        positions += (runner -> "3")
      }
      case _ =>
    }
    play match {
      case threeScoresExpression(firstScore, secondScore, thirdScore) => {
        val firstRunner = playerFromString(firstScore, team, year)
        advancements = advancements + positions(firstRunner) + "-H;"
        positions -= firstRunner
        val secondRunner = playerFromString(secondScore, team, year)
        advancements = advancements + positions(secondRunner) + "-H;"
        positions -= secondRunner
        val thirdRunner = playerFromString(thirdScore, team, year)
        advancements = advancements + positions(thirdRunner) + "-H;"
        positions -= thirdRunner
      }
      case twoScoresExpression(firstScore, secondScore) => {
        val firstRunner = playerFromString(firstScore, team, year)
        advancements = advancements + positions(firstRunner) + "-H;"
        positions -= firstRunner
        val secondRunner = playerFromString(secondScore, team, year)
        advancements = advancements + positions(secondRunner) + "-H;"
        positions -= secondRunner
      }
      case scoresExpression(name) => {
        val runner = playerFromString(name, team, year)
        advancements = advancements + positions(runner) + "-H;"
        positions -= runner
      }
      case _ =>
    }
    play match {
      case outAt2ndExpression(name) => positions -= playerFromString(name, team, year)
      case _                        =>
    }
    play match {
      case outAt3rdExpression(name) => positions -= playerFromString(name, team, year)
      case _                        =>
    }
    play match {
      case outAtHomeExpression(name) => positions -= playerFromString(name, team, year)
      case _                         =>
    }
    if (advancements.length > 1) advancements
    else ""
  }

  def clear = {
    positions = Map.empty[Player, String]
  }

}
