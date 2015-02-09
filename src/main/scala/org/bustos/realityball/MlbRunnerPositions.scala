package org.bustos.realityball

import RealityballRecords._

class MlbRunnerPositions {

  val realityballData = new RealityballData

  var positions = Map.empty[Player, String]

  val theJs = """\. *(A.  J.  )?(C.  J.  )?(B.  J.  )?(L.  J.  )?(Juan Carlos)?(J.  )?"""
  val nameExpression = """(A.  J.  )?(C.  J.  )?(B.  J.  )?(L.  J.  )?(Juan Carlos)?(J.  )?(.*?) (.*)""".r
  val outAt2ndExpression = """.*\.  *(.*) out at 2nd.*""".r
  val outAt3rdExpression = """.*\.  *(.*) out at 3rd.*""".r
  val outAtHomeExpression = """.*\.  *(.*) out at home.*""".r
  val to1stExpression = """.*\.  *(.*) to 1st.*""".r
  val to2ndExpression = """.*\.  *(.*) to 2nd.*""".r
  val to3rdExpression = """.*\.  *(.*) to 3rd.*""".r
  val advancesTo2ndExpression = """.*\.  *(.*) advances to 2nd.*""".r
  val advancesTo3rdExpression = """.*\.  *(.*) advances to 3rd.*""".r
  val scoresExpression = """.*\.  *(.*) scores.*""".r
  val twoScoresExpression = """.*\.  *(.*) scores\.  *(.*) scores.*""".r
  val threeScoresExpression = """.*\.  *(.*) scores\.  *(.*) scores\.  *(.*) scores.*""".r

  def lastName(name: String): String = {
    name match {
      case nameExpression(aj, cj, bj, lj, jc, jj, firstName, lastName) => lastName
      case _ => ""
    }
  }

  def playerFromString(name: String, team: String, year: String): Player = {
    val trimmedName = name.replace(" Jr.", "").trim
    trimmedName match {
      case nameExpression(aj, cj, bj, lj, jc, jj, firstName, lastName) => {
        val fName: String = {
          if (aj != null) aj
          else if (cj != null) cj
          else if (bj != null) bj
          else if (lj != null) lj
          else firstName
        }
        try {
          realityballData.playerFromName(fName(0).toString, lastName, year, team)
        } catch {
          case e: Exception => {
            println(trimmedName)
            realityballData.playerFromName(fName, lastName, year, team)
          }
        }
      }
      case _ => {
        println(trimmedName); null
      }
    }
  }

  def advancementString(play: String, batter: Player, team: String, year: String): String = {
    val trimmedPlay = play.replace(" jr.", "")
    var advancements = "."
    if (trimmedPlay.contains("single")) positions += (batter -> "1")
    if (trimmedPlay.contains("walk")) positions += (batter -> "1")
    if (trimmedPlay.contains("double")) positions += (batter -> "2")
    if (trimmedPlay.contains("triple")) positions += (batter -> "3")
    if (trimmedPlay.contains("hit by pitch")) { positions += (batter -> "1"); advancements = advancements + "B-1;" }
    if (trimmedPlay.contains("reaches on a fielding error")) { positions += (batter -> "1"); advancements = advancements + "B-1;" }
    if (trimmedPlay.contains("reaches on a force attempt")) { positions += (batter -> "1"); advancements = advancements + "B-1;" }
    trimmedPlay match {
      case outAt2ndExpression(name) => positions -= playerFromString(name, team, year)
      case _                        =>
    }
    trimmedPlay match {
      case outAt3rdExpression(name) => positions -= playerFromString(name, team, year)
      case _                        =>
    }
    trimmedPlay match {
      case outAtHomeExpression(name) => positions -= playerFromString(name, team, year)
      case _                         =>
    }
    trimmedPlay match {
      case to1stExpression(name) =>
        positions += (playerFromString(name, team, year) -> "1"); advancements = advancements + "B-1;"
      case _ =>
    }
    trimmedPlay match {
      case advancesTo2ndExpression(name) => {
        val runner = playerFromString(name, team, year)
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
    trimmedPlay match {
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
    trimmedPlay match {
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
    if (advancements.length > 1) advancements
    else ""
  }

  def clear = {
    positions = Map.empty[Player, String]
  }

}
